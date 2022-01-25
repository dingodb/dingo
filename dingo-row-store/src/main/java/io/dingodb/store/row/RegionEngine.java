/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.store.row;

import io.dingodb.raft.Lifecycle;
import io.dingodb.raft.Node;
import io.dingodb.raft.RaftGroupService;
import io.dingodb.raft.RouteTable;
import io.dingodb.raft.Status;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.option.NodeOptions;
import io.dingodb.raft.rpc.RpcServer;
import io.dingodb.raft.util.Describer;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.raft.util.Requires;
import io.dingodb.raft.util.internal.ThrowUtil;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import io.dingodb.store.row.client.pd.RegionHeartbeatSender;
import io.dingodb.store.row.client.pd.RemotePlacementDriverClient;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.store.row.options.HeartbeatOptions;
import io.dingodb.store.row.options.RegionEngineOptions;
import io.dingodb.store.row.storage.KVStoreStateMachine;
import io.dingodb.store.row.storage.MetricsRawKVStore;
import io.dingodb.store.row.storage.RaftRawKVStore;
import io.dingodb.store.row.storage.RawKVStore;
import io.dingodb.store.row.util.Strings;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class RegionEngine implements Lifecycle<RegionEngineOptions>, Describer, StateListener {
    private static final Logger LOG = LoggerFactory.getLogger(RegionEngine.class);

    private final Region region;
    private final StoreEngine storeEngine;

    private RaftRawKVStore raftRawKVStore;
    private MetricsRawKVStore metricsRawKVStore;
    private RaftGroupService raftGroupService;
    private Node node;
    private KVStoreStateMachine fsm;
    private RegionEngineOptions regionOpts;

    private ScheduledReporter regionMetricsReporter;
    private RegionHeartbeatSender heartbeatSender;

    private boolean started;

    public RegionEngine(Region region, StoreEngine storeEngine) {
        this.region = region;
        this.storeEngine = storeEngine;
    }

    @Override
    public synchronized boolean init(final RegionEngineOptions opts) {
        if (this.started) {
            LOG.info("[RegionEngine: {}] already started.", this.region);
            return true;
        }
        this.regionOpts = Requires.requireNonNull(opts, "opts");
        this.fsm = new KVStoreStateMachine(this.region, this.storeEngine);
        this.storeEngine.getStateListenerContainer().addStateListener(this.region.getId(), this);

        // node options
        NodeOptions nodeOpts = opts.getNodeOptions();
        if (nodeOpts == null) {
            nodeOpts = new NodeOptions();
        }
        final long metricsReportPeriod = opts.getMetricsReportPeriod();
        if (metricsReportPeriod > 0) {
            // metricsReportPeriod > 0 means enable metrics
            nodeOpts.setEnableMetrics(true);
        }
        final Configuration initialConf = new Configuration();
        if (!initialConf.parse(opts.getInitialServerList())) {
            LOG.error("Fail to parse initial configuration {}.", opts.getInitialServerList());
            return false;
        }
        nodeOpts.setInitialConf(initialConf);
        nodeOpts.setFsm(this.fsm);
        final String raftDataPath = opts.getRaftDataPath();
        try {
            FileUtils.forceMkdir(new File(raftDataPath));
        } catch (final Throwable t) {
            LOG.error("Fail to make dir for raftDataPath {}.", raftDataPath);
            return false;
        }
        if (Strings.isBlank(nodeOpts.getLogUri())) {
            final Path logUri = Paths.get(raftDataPath, "log");
            nodeOpts.setLogUri(logUri.toString());
        }
        if (Strings.isBlank(nodeOpts.getRaftMetaUri())) {
            final Path meteUri = Paths.get(raftDataPath, "meta");
            nodeOpts.setRaftMetaUri(meteUri.toString());
        }
        if (Strings.isBlank(nodeOpts.getSnapshotUri())) {
            final Path snapshotUri = Paths.get(raftDataPath, "snapshot");
            nodeOpts.setSnapshotUri(snapshotUri.toString());
        }
        LOG.info("[RegionEngine: {}], log uri: {}, raft meta uri: {}, snapshot uri: {}.", this.region,
            nodeOpts.getLogUri(), nodeOpts.getRaftMetaUri(), nodeOpts.getSnapshotUri());
        final Endpoint serverAddress = opts.getServerAddress();
        final PeerId serverId = new PeerId(serverAddress, 0);
        final RpcServer rpcServer = this.storeEngine.getRpcServer();
        this.raftGroupService = new RaftGroupService(opts.getRaftGroupId(), serverId, nodeOpts, rpcServer, true);
        this.node = this.raftGroupService.start(false);
        RouteTable.getInstance().updateConfiguration(this.raftGroupService.getGroupId(), nodeOpts.getInitialConf());
        if (this.node != null) {
            final RawKVStore rawKVStore = this.storeEngine.getRawKVStore();
            final Executor readIndexExecutor = this.storeEngine.getReadIndexExecutor();
            this.raftRawKVStore = new RaftRawKVStore(this.node, rawKVStore, readIndexExecutor);
            this.metricsRawKVStore = new MetricsRawKVStore(this.region.getId(), this.raftRawKVStore);
            // metrics config
            if (this.regionMetricsReporter == null && metricsReportPeriod > 0) {
                final MetricRegistry metricRegistry = this.node.getNodeMetrics().getMetricRegistry();
                if (metricRegistry != null) {
                    final ScheduledExecutorService scheduler = this.storeEngine.getMetricsScheduler();
                    // start raft node metrics reporter
                    this.regionMetricsReporter = Slf4jReporter.forRegistry(metricRegistry)
                        .prefixedWith("region_" + this.region.getId())
                        .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO)
                        .outputTo(LOG)
                        .scheduleOn(scheduler)
                        .shutdownExecutorOnStop(scheduler != null)
                        .build();
                    this.regionMetricsReporter.start(metricsReportPeriod, TimeUnit.SECONDS);
                }
            }
            if (this.storeEngine.getPlacementDriverClient() instanceof RemotePlacementDriverClient) {
                HeartbeatOptions heartbeatOpts = opts.getHeartbeatOptions();
                if (heartbeatOpts == null) {
                    heartbeatOpts = new HeartbeatOptions();
                }
                this.heartbeatSender = new RegionHeartbeatSender(this);
                if (!this.heartbeatSender.init(heartbeatOpts)) {
                    LOG.error("Fail to init [HeartbeatSender].");
                    return false;
                }
            }
            this.started = true;
            LOG.info("[RegionEngine] start successfully: {}.", this);
        }
        return this.started;
    }

    @Override
    public synchronized void shutdown() {
        if (!this.started) {
            return;
        }
        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            try {
                this.raftGroupService.join();
            } catch (final InterruptedException e) {
                ThrowUtil.throwException(e);
            }
        }
        if (this.regionMetricsReporter != null) {
            this.regionMetricsReporter.stop();
        }
        this.started = false;
        LOG.info("[RegionEngine] shutdown successfully: {}.", this);
    }

    public boolean transferLeadershipTo(final Endpoint endpoint) {
        final PeerId peerId = new PeerId(endpoint, 0);
        final Status status = this.node.transferLeadershipTo(peerId);
        final boolean isOk = status.isOk();
        if (isOk) {
            LOG.info("Transfer-leadership succeeded: [{} --> {}].", this.storeEngine.getSelfEndpoint(), endpoint);
        } else {
            LOG.error("Transfer-leadership failed: {}, [{} --> {}].", status, this.storeEngine.getSelfEndpoint(),
                endpoint);
        }
        return isOk;
    }

    public Region getRegion() {
        return region;
    }

    public StoreEngine getStoreEngine() {
        return storeEngine;
    }

    public boolean isLeader() {
        return this.node.isLeader(false);
    }

    public PeerId getLeaderId() {
        return this.node.getLeaderId();
    }

    public RaftRawKVStore getRaftRawKVStore() {
        return raftRawKVStore;
    }

    public MetricsRawKVStore getMetricsRawKVStore() {
        return metricsRawKVStore;
    }

    public Node getNode() {
        return node;
    }

    public KVStoreStateMachine getFsm() {
        return fsm;
    }

    public RegionEngineOptions copyRegionOpts() {
        return Requires.requireNonNull(this.regionOpts, "opts").copy();
    }


    @Override
    public void onLeaderStart(long newTerm) {
        if (heartbeatSender != null) {
            heartbeatSender.start();
        }
    }

    @Override
    public void onLeaderStop(long oldTerm) {
        if (heartbeatSender != null) {
            heartbeatSender.shutdown();
        }
    }

    @Override
    public void onStartFollowing(PeerId newLeaderId, long newTerm) {
        if (heartbeatSender != null) {
            heartbeatSender.shutdown();
        }
    }

    @Override
    public void onStopFollowing(PeerId oldLeaderId, long oldTerm) {
        if (heartbeatSender != null) {
            heartbeatSender.shutdown();
        }
    }


    @Override
    public String toString() {
        return "RegionEngine{" + "region=" + region + ", isLeader=" + isLeader() + ", regionOpts=" + regionOpts + '}';
    }

    @Override
    public void describe(final Printer out) {
        out.print("  RegionEngine: ") //
            .print("regionId=") //
            .print(this.region.getId()) //
            .print(", isLeader=") //
            .print(isLeader()) //
            .print(", leaderId=") //
            .println(getLeaderId());
    }
}
