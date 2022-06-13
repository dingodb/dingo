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

package io.dingodb.server.coordinator.state;

import com.google.protobuf.ByteString;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.raft.Closure;
import io.dingodb.raft.Iterator;
import io.dingodb.raft.Node;
import io.dingodb.raft.StateMachine;
import io.dingodb.raft.Status;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.core.NodeImpl;
import io.dingodb.raft.entity.LeaderChangeContext;
import io.dingodb.raft.entity.LocalFileMetaOutter.LocalFileMeta;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.error.RaftException;
import io.dingodb.raft.kv.storage.RaftClosure;
import io.dingodb.raft.kv.storage.RaftRawKVOperation;
import io.dingodb.raft.kv.storage.RawKVStore;
import io.dingodb.raft.rpc.ReportTarget;
import io.dingodb.raft.rpc.impl.AbstractClientService;
import io.dingodb.raft.storage.snapshot.SnapshotReader;
import io.dingodb.raft.storage.snapshot.SnapshotWriter;
import io.dingodb.server.coordinator.api.CoordinatorServerApi;
import io.dingodb.server.coordinator.api.ScheduleApi;
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
import io.dingodb.server.coordinator.meta.adaptor.impl.BaseAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.BaseStatsAdaptor;
import io.dingodb.server.coordinator.metric.PartMetricCollector;
import io.dingodb.server.coordinator.schedule.ClusterScheduler;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.Tags;
import io.prometheus.client.exporter.HTTPServer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.zip.Checksum;

import static io.dingodb.raft.kv.Constants.SNAPSHOT_ZIP;

@Slf4j
public class CoordinatorStateMachine implements StateMachine {

    private final Node node;

    private final RawKVStore cache;
    private final RawKVStore store;

    private final MetaStore metaStore;

    private final NetService netService;
    private CoordinatorServerApi serverApi;
    private ScheduleApi scheduleApi;
    private final Set<Channel> leaderListener = new CopyOnWriteArraySet<>();

    public CoordinatorStateMachine(Node node, RawKVStore cache, RawKVStore store, MetaStore metaStore) {
        this.node = node;
        this.cache = cache;
        this.store = store;
        this.metaStore = metaStore;
        this.netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
        this.netService.registerTagMessageListener(Tags.LISTEN_RAFT_LEADER, this::onMessage);
    }

    @Override
    public void onApply(Iterator iterator) {
        int applied = 0;
        try {
            while (iterator.hasNext()) {
                Closure done = iterator.done();
                try {
                    RaftRawKVOperation operation = RaftRawKVOperation.decode(iterator.getData());
                    Object result = execute(operation);
                    if (done instanceof RaftClosure) {
                        ((RaftClosure<?>) done).complete(result);
                    }
                    applied++;
                } catch (Exception e) {
                    iterator.next();
                    done.run(new Status(-1, ""));
                    throw e;
                }
                iterator.next();
            }
        } catch (final Throwable t) {
            log.error("StateMachine meet critical error: {}.", t.getMessage(), t);
            iterator.setErrorAndRollback(iterator.getIndex() - applied, new Status(
                RaftError.ESTATEMACHINE,
                "StateMachine meet critical error: %s.", t.getMessage()));
        }
    }

    private RawKVStore getStore(RaftRawKVOperation operation) {
        return store;
    }

    private Object execute(RaftRawKVOperation operation) {
        RawKVStore store = getStore(operation);
        switch (operation.getOp()) {
            case SYNC:
                return true;
            case PUT:
                store.put(operation.getKey(), operation.getValue());
                return true;
            case PUT_LIST:
                store.put(operation.ext1());
                return true;
            case DELETE:
                return store.delete(operation.getKey());
            case DELETE_LIST:
                return store.delete((List<byte[]>) operation.ext1());
            case DELETE_RANGE:
                return store.delete(operation.getKey(), operation.getValue());
            default:
                throw new IllegalStateException("Not write or sync operation: " + operation.getOp());
        }
    }

    @Override
    public void onShutdown() {
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done, ReportTarget reportTarget) {
        onSnapshotSave(writer, done);
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        Checksum checksum = store.snapshotSave(writer.getPath()).join();
        LocalFileMeta.Builder metaBuilder = LocalFileMeta.newBuilder();
        metaBuilder.setChecksum(Long.toHexString(checksum.getValue()));
        metaBuilder.setUserMeta(ByteString.copyFromUtf8(node.getGroupId()));
        writer.addFile(SNAPSHOT_ZIP, metaBuilder.build());
        done.run(Status.OK());
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        return store
            .snapshotLoad(reader.getPath(), ((LocalFileMeta) reader.getFileMeta(SNAPSHOT_ZIP)).getChecksum())
            .join();
    }

    @Override
    public void onReportFreezeSnapshotResult(boolean freezeResult, String errMsg, ReportTarget reportTarget) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRegionId() {
        throw new UnsupportedOperationException();
    }

    public boolean isLeader() {
        return node.isLeader();
    }

    @Override
    public void onLeaderStart(final long term) {
        log.info("onLeaderStart: term={}.", term);
        if (serverApi == null) {
            this.serverApi = new CoordinatorServerApi(node,
                ((AbstractClientService) ((NodeImpl) node).getRpcService()).getRpcClient(),
                netService
            );
        }
        if (scheduleApi == null) {
            scheduleApi = new ScheduleApi();
        }
        leaderListener.forEach(channel -> Executors.submit("leader-notify", () -> {
            log.info("Send leader message to [{}].", channel.remoteLocation().getUrl());
            channel.send(Message.EMPTY);
        }));
        Executors.submit("on-leader", () -> {
            ServiceLoader.load(BaseAdaptor.Creator.class).iterator()
                .forEachRemaining(creator -> creator.create(metaStore));
            ServiceLoader.load(BaseStatsAdaptor.Creator.class).iterator()
                .forEachRemaining(creator -> creator.create(metaStore));
            ClusterScheduler.instance().init();
            try {
                HTTPServer httpServer = new HTTPServer(CoordinatorConfiguration.monitorPort());
                new PartMetricCollector().register();
            } catch (IOException e) {
                log.error("http server error", e);
            }
        });
    }

    @Override
    public void onLeaderStop(final Status status) {
        log.info("onLeaderStop: status={}.", status);
    }

    @Override
    public void onError(final RaftException ex) {
        log.error(
            "Encountered {} on {}, you should figure out the cause and repair or remove this node.",
            ex.getStatus(), getClass().getName(), ex
        );
    }

    @Override
    public void onConfigurationCommitted(final Configuration conf) {
        log.info("onConfigurationCommitted: {}.", conf);
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        log.info("onStopFollowing: {}.", ctx);
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        log.info("onStartFollowing: {}.", ctx);
        if (serverApi == null) {
            this.serverApi = new CoordinatorServerApi(node,
                ((AbstractClientService) ((NodeImpl) node).getRpcService()).getRpcClient(),
                netService
            );
        }
    }

    private void onMessage(Message message, Channel channel) {
        log.info("New leader listener channel, remote: [{}]", channel.remoteLocation());
        leaderListener.add(channel);
        channel.send(Message.EMPTY);
        if (node.isLeader()) {
            channel.send(Message.EMPTY);
        }
        channel.closeListener(leaderListener::remove);
    }

}
