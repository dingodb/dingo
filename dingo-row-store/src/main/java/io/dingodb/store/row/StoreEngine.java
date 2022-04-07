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

import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import io.dingodb.raft.Lifecycle;
import io.dingodb.raft.Status;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.entity.Task;
import io.dingodb.raft.option.NodeOptions;
import io.dingodb.raft.rpc.RaftRpcServerFactory;
import io.dingodb.raft.rpc.RpcServer;
import io.dingodb.raft.util.BytesUtil;
import io.dingodb.raft.util.Describer;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.raft.util.ExecutorServiceHelper;
import io.dingodb.raft.util.Requires;
import io.dingodb.raft.util.ThreadPoolMetricRegistry;
import io.dingodb.raft.util.Utils;
import io.dingodb.store.row.client.pd.PlacementDriverClient;
import io.dingodb.store.row.errors.DingoRowStoreRuntimeException;
import io.dingodb.store.row.errors.Errors;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.store.row.metadata.RegionEpoch;
import io.dingodb.store.row.metadata.Store;
import io.dingodb.store.row.metrics.KVMetrics;
import io.dingodb.store.row.options.*;
import io.dingodb.store.row.rpc.CompareRegionService;
import io.dingodb.store.row.rpc.ExtSerializerSupports;
import io.dingodb.store.row.rpc.ReportToLeaderService;
import io.dingodb.store.row.serialization.Serializers;
import io.dingodb.store.row.storage.BatchRawKVStore;
import io.dingodb.store.row.storage.KVClosureAdapter;
import io.dingodb.store.row.storage.KVOperation;
import io.dingodb.store.row.storage.KVStoreClosure;
import io.dingodb.store.row.storage.MemoryRawKVStore;
import io.dingodb.store.row.storage.RocksRawKVStore;
import io.dingodb.store.row.storage.StorageType;
import io.dingodb.store.row.util.Constants;
import io.dingodb.store.row.util.Lists;
import io.dingodb.store.row.util.Maps;
import io.dingodb.store.row.util.NetUtil;
import io.dingodb.store.row.util.Strings;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class StoreEngine implements Lifecycle<StoreEngineOptions>, Describer {
    private static final Logger LOG = LoggerFactory.getLogger(StoreEngine.class);

    static {
        ExtSerializerSupports.init();
    }

    private final ConcurrentMap<String, RegionKVService> regionKVServiceTable = Maps.newConcurrentMap();
    private final ConcurrentMap<String, RegionEngine> regionEngineTable = Maps.newConcurrentMap();
    private final StateListenerContainer<String> stateListenerContainer;
    private final PlacementDriverClient pdClient;
    private final long clusterId;

    private String storeId;
    private final AtomicBoolean splitting = new AtomicBoolean(false);
    // When the store is started (unix timestamp in milliseconds)
    private long startTime = System.currentTimeMillis();
    private File dbPath;
    private RpcServer rpcServer;
    private BatchRawKVStore<?> rawKVStore;
    private StoreEngineOptions storeOpts;

    // Shared executor services
    private ExecutorService readIndexExecutor;
    private ExecutorService raftStateTrigger;
    private ExecutorService snapshotExecutor;
    private ExecutorService cliRpcExecutor;
    private ExecutorService raftRpcExecutor;
    private ExecutorService kvRpcExecutor;

    private ScheduledExecutorService metricsScheduler;
    private ScheduledReporter kvMetricsReporter;
    private ScheduledReporter threadPoolMetricsReporter;

    private boolean started;

    public StoreEngine(PlacementDriverClient pdClient, StateListenerContainer<String> stateListenerContainer) {
        this.pdClient = Requires.requireNonNull(pdClient, "pdClient");
        this.clusterId = pdClient.getClusterId();
        this.stateListenerContainer = Requires.requireNonNull(stateListenerContainer, "stateListenerContainer");
    }

    @Override
    public synchronized boolean init(final StoreEngineOptions opts) {
        if (this.started) {
            LOG.info("[StoreEngine] already started.");
            return true;
        }

        DescriberManager.getInstance().addDescriber(this);

        this.storeOpts = Requires.requireNonNull(opts, "opts");
        Endpoint serverAddress = Requires.requireNonNull(opts.getServerAddress(), "opts.serverAddress");
        final int port = serverAddress.getPort();
        final String ip = serverAddress.getIp();
        if (ip == null || Utils.IP_ANY.equals(ip)) {
            serverAddress = new Endpoint(NetUtil.getLocalCanonicalHostName(), port);
            opts.setServerAddress(serverAddress);
        }
        final long metricsReportPeriod = opts.getMetricsReportPeriod();
        // init region options
        List<RegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        if (rOptsList == null || rOptsList.isEmpty()) {
            // -1 region
            final RegionEngineOptions rOpts = new RegionEngineOptions();
            rOpts.setRegionId(Constants.DEFAULT_REGION_ID);
            rOptsList = Lists.newArrayList();
            rOptsList.add(rOpts);
            opts.setRegionEngineOptionsList(rOptsList);
        }
        final String clusterName = this.pdClient.getClusterName();
        for (final RegionEngineOptions rOpts : rOptsList) {
            rOpts.setRaftGroupId(JRaftHelper.getJRaftGroupId(clusterName, rOpts.getRegionId()));
            rOpts.setServerAddress(serverAddress);
            if (Strings.isBlank(rOpts.getInitialServerList())) {
                // if blank, extends parent's value
                rOpts.setInitialServerList(opts.getInitialServerList());
            }
            if (rOpts.getNodeOptions() == null) {
                // copy common node options
                rOpts.setNodeOptions(opts.getCommonNodeOptions() == null ? new NodeOptions() : opts
                    .getCommonNodeOptions().copy());
            }
            if (rOpts.getMetricsReportPeriod() <= 0 && metricsReportPeriod > 0) {
                // extends store opts
                rOpts.setMetricsReportPeriod(metricsReportPeriod);
            }
        }
        // init store
        final Store store = this.pdClient.getStoreMetadata(opts);
        if (store == null || store.getRegions() == null || store.getRegions().isEmpty()) {
            LOG.error("Empty store metadata: {}.", store);
            return false;
        }
        this.storeId = store.getId();
        // init executors
        if (this.readIndexExecutor == null) {
            this.readIndexExecutor = StoreEngineHelper.createReadIndexExecutor(opts.getReadIndexCoreThreads());
        }
        if (this.raftStateTrigger == null) {
            this.raftStateTrigger = StoreEngineHelper.createRaftStateTrigger(opts.getLeaderStateTriggerCoreThreads());
        }
        if (this.snapshotExecutor == null) {
            this.snapshotExecutor = StoreEngineHelper.createSnapshotExecutor(opts.getSnapshotCoreThreads(),
                opts.getSnapshotMaxThreads());
        }
        // init rpc executors
        final boolean useSharedRpcExecutor = opts.isUseSharedRpcExecutor();
        if (!useSharedRpcExecutor) {
            if (this.cliRpcExecutor == null) {
                this.cliRpcExecutor = StoreEngineHelper.createCliRpcExecutor(opts.getCliRpcCoreThreads());
            }
            if (this.raftRpcExecutor == null) {
                this.raftRpcExecutor = StoreEngineHelper.createRaftRpcExecutor(opts.getRaftRpcCoreThreads());
            }
            if (this.kvRpcExecutor == null) {
                this.kvRpcExecutor = StoreEngineHelper.createKvRpcExecutor(opts.getKvRpcCoreThreads());
            }
        }
        // init metrics
        startMetricReporters(metricsReportPeriod);
        // init rpc server
        this.rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverAddress, this.raftRpcExecutor,
            this.cliRpcExecutor);
        StoreEngineHelper.addKvStoreRequestProcessor(this.rpcServer, this);
        if (!this.rpcServer.init(null)) {
            LOG.error("Fail to init [RpcServer].");
            return false;
        }
        // init db store
        if (!initRawKVStore(opts)) {
            return false;
        }
        if (this.rawKVStore instanceof Describer) {
            DescriberManager.getInstance().addDescriber((Describer) this.rawKVStore);
        }
        // init all region engine
        if (!initAllRegionEngine(opts, store)) {
            LOG.error("Fail to init all [RegionEngine].");
            return false;
        }
        ReportToLeaderService.instance().init(this);
        CompareRegionService.instance().init(this);
        this.startTime = System.currentTimeMillis();
        LOG.info("[StoreEngine] start successfully: {}.", this);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (!this.started) {
            return;
        }
        if (this.rpcServer != null) {
            this.rpcServer.shutdown();
        }
        if (!this.regionEngineTable.isEmpty()) {
            for (final RegionEngine engine : this.regionEngineTable.values()) {
                engine.shutdown();
            }
            this.regionEngineTable.clear();
        }
        if (this.rawKVStore != null) {
            this.rawKVStore.shutdown();
        }
        this.regionKVServiceTable.clear();
        if (this.kvMetricsReporter != null) {
            this.kvMetricsReporter.stop();
        }
        if (this.threadPoolMetricsReporter != null) {
            this.threadPoolMetricsReporter.stop();
        }
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.readIndexExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.raftStateTrigger);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.snapshotExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.cliRpcExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.raftRpcExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.kvRpcExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.metricsScheduler);
        this.started = false;
        LOG.info("[StoreEngine] shutdown successfully.");
    }

    public PlacementDriverClient getPlacementDriverClient() {
        return pdClient;
    }

    public long getClusterId() {
        return clusterId;
    }

    public String getStoreId() {
        return storeId;
    }

    public StoreEngineOptions getStoreOpts() {
        return storeOpts;
    }

    public long getStartTime() {
        return startTime;
    }

    public RpcServer getRpcServer() {
        return rpcServer;
    }

    public BatchRawKVStore<?> getRawKVStore() {
        return rawKVStore;
    }

    public RegionKVService getRegionKVService(final String regionId) {
        return this.regionKVServiceTable.get(regionId);
    }

    public long getTotalSpace() {
        if (this.dbPath == null || !this.dbPath.exists()) {
            return 0;
        }
        return this.dbPath.getTotalSpace();
    }

    public long getUsableSpace() {
        if (this.dbPath == null || !this.dbPath.exists()) {
            return 0;
        }
        return this.dbPath.getUsableSpace();
    }

    public long getStoreUsedSpace() {
        if (this.dbPath == null || !this.dbPath.exists()) {
            return 0;
        }
        return FileUtils.sizeOf(this.dbPath);
    }

    public Endpoint getSelfEndpoint() {
        return this.storeOpts == null ? null : this.storeOpts.getServerAddress();
    }

    public RegionEngine getRegionEngine(final String regionId) {
        return this.regionEngineTable.get(regionId);
    }

    public List<RegionEngine> getAllRegionEngines() {
        return Lists.newArrayList(this.regionEngineTable.values());
    }

    public ExecutorService getReadIndexExecutor() {
        return readIndexExecutor;
    }

    public void setReadIndexExecutor(ExecutorService readIndexExecutor) {
        this.readIndexExecutor = readIndexExecutor;
    }

    public ExecutorService getRaftStateTrigger() {
        return raftStateTrigger;
    }

    public void setRaftStateTrigger(ExecutorService raftStateTrigger) {
        this.raftStateTrigger = raftStateTrigger;
    }

    public ExecutorService getSnapshotExecutor() {
        return snapshotExecutor;
    }

    public void setSnapshotExecutor(ExecutorService snapshotExecutor) {
        this.snapshotExecutor = snapshotExecutor;
    }

    public ExecutorService getCliRpcExecutor() {
        return cliRpcExecutor;
    }

    public void setCliRpcExecutor(ExecutorService cliRpcExecutor) {
        this.cliRpcExecutor = cliRpcExecutor;
    }

    public ExecutorService getRaftRpcExecutor() {
        return raftRpcExecutor;
    }

    public void setRaftRpcExecutor(ExecutorService raftRpcExecutor) {
        this.raftRpcExecutor = raftRpcExecutor;
    }

    public ExecutorService getKvRpcExecutor() {
        return kvRpcExecutor;
    }

    public void setKvRpcExecutor(ExecutorService kvRpcExecutor) {
        this.kvRpcExecutor = kvRpcExecutor;
    }

    public ScheduledExecutorService getMetricsScheduler() {
        return metricsScheduler;
    }

    public void setMetricsScheduler(ScheduledExecutorService metricsScheduler) {
        this.metricsScheduler = metricsScheduler;
    }

    public ScheduledReporter getKvMetricsReporter() {
        return kvMetricsReporter;
    }

    public void setKvMetricsReporter(ScheduledReporter kvMetricsReporter) {
        this.kvMetricsReporter = kvMetricsReporter;
    }

    public ScheduledReporter getThreadPoolMetricsReporter() {
        return threadPoolMetricsReporter;
    }

    public void setThreadPoolMetricsReporter(ScheduledReporter threadPoolMetricsReporter) {
        this.threadPoolMetricsReporter = threadPoolMetricsReporter;
    }

    public boolean removeAndStopRegionEngine(final long regionId) {
        final RegionEngine engine = this.regionEngineTable.get(regionId);
        if (engine != null) {
            engine.shutdown();
            return true;
        }
        return false;
    }

    public StateListenerContainer<String> getStateListenerContainer() {
        return stateListenerContainer;
    }

    public List<String> getLeaderRegionIds() {
        final List<String> regionIds = Lists.newArrayListWithCapacity(this.regionEngineTable.size());
        for (final RegionEngine regionEngine : this.regionEngineTable.values()) {
            if (regionEngine.isLeader()) {
                regionIds.add(regionEngine.getRegion().getId());
            }
        }
        return regionIds;
    }

    public int getRegionCount() {
        return this.regionEngineTable.size();
    }

    public int getLeaderRegionCount() {
        int count = 0;
        for (final RegionEngine regionEngine : this.regionEngineTable.values()) {
            if (regionEngine.isLeader()) {
                count++;
            }
        }
        return count;
    }

    public boolean isBusy() {
        // Need more info
        return splitting.get();
    }

    public void applySplit(final String regionId, final String newRegionId, final KVStoreClosure closure) {
        Requires.requireNonNull(regionId, "regionId");
        Requires.requireNonNull(newRegionId, "newRegionId");
        if (this.regionEngineTable.containsKey(newRegionId)) {
            closure.setError(Errors.CONFLICT_REGION_ID);
            closure.run(new Status(-1, "Conflict region id %d", newRegionId));
            return;
        }
        if (!this.splitting.compareAndSet(false, true)) {
            closure.setError(Errors.SERVER_BUSY);
            closure.run(new Status(-1, "Server is busy now"));
            return;
        }
        final RegionEngine parentEngine = getRegionEngine(regionId);
        if (parentEngine == null) {
            closure.setError(Errors.NO_REGION_FOUND);
            closure.run(new Status(-1, "RegionEngine[%s] not found", regionId));
            this.splitting.set(false);
            return;
        }
        if (!parentEngine.isLeader()) {
            closure.setError(Errors.NOT_LEADER);
            closure.run(new Status(-1, "RegionEngine[%s] not leader", regionId));
            this.splitting.set(false);
            return;
        }
        final Region parentRegion = parentEngine.getRegion();
        final byte[] startKey = BytesUtil.nullToEmpty(parentRegion.getStartKey());
        final byte[] endKey = parentRegion.getEndKey();
        ApproximateKVStats stats = this.rawKVStore.getApproximateKVStatsInRange(startKey, endKey);
        final long approximateKeys =  stats.keysCnt;
        final long approximateBytes = stats.sizeInBytes / 1024 / 1024;
        final long leastKeysOnSplit = this.storeOpts.getLeastKeysOnSplit();

        boolean isSplitOK = (approximateBytes >= 64 || approximateKeys > leastKeysOnSplit);
        LOG.info("Region:{} Split Condition is {}!. Disk Used:{} >= 64M, Write Keys:{} >= Expected Keys:{}",
            parentEngine,
            isSplitOK,
            approximateBytes,
            approximateKeys,
            leastKeysOnSplit);

        if (!isSplitOK) {
            closure.setError(Errors.TOO_SMALL_TO_SPLIT);
            closure.run(new Status(-1, "RegionEngine[%s]'s split condition is not OK!. "
                + "Write Keys:%d bytes(M): %d, Expected: keys:%d, bytes: 64M",
                regionId,
                approximateKeys,
                approximateBytes,
                leastKeysOnSplit));
            this.splitting.set(false);
            return;
        }
        final byte[] splitKey = this.rawKVStore.jumpOver(startKey, approximateKeys >> 1);
        if (splitKey == null) {
            closure.setError(Errors.STORAGE_ERROR);
            closure.run(new Status(-1, "Fail to scan split key"));
            this.splitting.set(false);
            return;
        }
        final KVOperation op = KVOperation.createRangeSplit(splitKey, regionId, newRegionId);
        LOG.info("Store receive region split instruction: Old Region:{}, oldStartKey:{}, oldEndKey:{}, "
                + "approximateKeys:{}, newRegionId:{}, splitKey:{}",
            parentEngine.toString(),
            startKey != null ? BytesUtil.toHex(startKey) : "null",
            endKey != null ? BytesUtil.toHex(endKey) : "null",
            approximateKeys,
            newRegionId,
            splitKey != null ? BytesUtil.toHex(splitKey) : "null");
        final Task task = new Task();
        task.setData(ByteBuffer.wrap(Serializers.getDefault().writeObject(op)));
        task.setDone(new KVClosureAdapter(closure, op));
        parentEngine.getNode().apply(task);
    }

    public void doSplit(final String regionId, final String newRegionId, final byte[] splitKey) {
        try {
            Requires.requireNonNull(regionId, "regionId");
            Requires.requireNonNull(newRegionId, "newRegionId");
            final RegionEngine parent = getRegionEngine(regionId);
            final Region region = parent.getRegion().copy();
            final RegionEngineOptions rOpts = parent.copyRegionOpts();
            region.setId(newRegionId);
            region.setStartKey(splitKey);
            region.setRegionEpoch(new RegionEpoch(-1, -1));

            rOpts.setRegionId(newRegionId);
            rOpts.setStartKeyBytes(region.getStartKey());
            rOpts.setEndKeyBytes(region.getEndKey());
            rOpts.setRaftGroupId(JRaftHelper.getJRaftGroupId(this.pdClient.getClusterName(), newRegionId));
            rOpts.setRaftDataPath(null);

            String baseRaftDataPath = "";
            if (this.storeOpts.getStoreDBOptions() != null) {
                baseRaftDataPath = this.storeOpts.getRaftStoreOptions().getDataPath();
            }
            String raftDataPath = JRaftHelper.getRaftDataPath(
                baseRaftDataPath,
                region.getId(),
                getSelfEndpoint().getPort());
            rOpts.setRaftDataPath(raftDataPath);
            rOpts.setRaftStoreOptions(this.storeOpts.getRaftStoreOptions());

            final RegionEngine engine = new RegionEngine(region, this);
            if (!engine.init(rOpts)) {
                LOG.error("Fail to init [RegionEngine: {}].", region);
                throw Errors.REGION_ENGINE_FAIL.exception();
            }

            // update parent conf
            final Region pRegion = parent.getRegion();
            final RegionEpoch pEpoch = pRegion.getRegionEpoch();
            final long version = pEpoch.getVersion();
            pEpoch.setVersion(version + 1); // version + 1
            pRegion.setEndKey(splitKey); // update endKey

            // the following two lines of code can make a relation of 'happens-before' for
            // read 'pRegion', because that a write to a ConcurrentMap happens-before every
            // subsequent read of that ConcurrentMap.
            this.regionEngineTable.put(region.getId(), engine);
            registerRegionKVService(new DefaultRegionKVService(engine));

            // update local regionRouteTable
            this.pdClient.getRegionRouteTable().splitRegion(pRegion.getId(), region);

            /**
             * when Region is split, then the cluster info should be update.
             * 1. using the split region to replace the old region
             * 2. insert the split new region
             * 3. call the pdClient to notify the placement driver.
             */
            // todo Huzx
            /*
            {
                Store localStore = this.pdClient.getCurrentStore();
                List<Region> regionList = new ArrayList<>();
                for (Map.Entry<Long, RegionEngine> entry : this.regionEngineTable.entrySet()) {
                    regionList.add(entry.getValue().getRegion().copy());
                }
                localStore.setRegions(regionList);
                this.pdClient.refreshStore(localStore);
            }
             */

        } finally {
            this.splitting.set(false);
        }
    }

    private void startMetricReporters(final long metricsReportPeriod) {
        if (metricsReportPeriod <= 0) {
            return;
        }
        if (this.kvMetricsReporter == null) {
            if (this.metricsScheduler == null) {
                // will sharing with all regionEngines
                this.metricsScheduler = StoreEngineHelper.createMetricsScheduler();
            }
            // start kv store metrics reporter
            this.kvMetricsReporter = Slf4jReporter.forRegistry(KVMetrics.metricRegistry()) //
                .prefixedWith("store_" + this.storeId) //
                .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO) //
                .outputTo(LOG) //
                .scheduleOn(this.metricsScheduler) //
                .shutdownExecutorOnStop(false) //
                .build();
            this.kvMetricsReporter.start(metricsReportPeriod, TimeUnit.SECONDS);
        }
        if (this.threadPoolMetricsReporter == null) {
            if (this.metricsScheduler == null) {
                // will sharing with all regionEngines
                this.metricsScheduler = StoreEngineHelper.createMetricsScheduler();
            }
            // start threadPool metrics reporter
            this.threadPoolMetricsReporter = Slf4jReporter.forRegistry(ThreadPoolMetricRegistry.metricRegistry()) //
                .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO) //
                .outputTo(LOG) //
                .scheduleOn(this.metricsScheduler) //
                .shutdownExecutorOnStop(false) //
                .build();
            this.threadPoolMetricsReporter.start(metricsReportPeriod, TimeUnit.SECONDS);
        }
    }

    private boolean initRawKVStore(final StoreEngineOptions opts) {
        final StorageType storageType = opts.getStorageType();
        switch (storageType) {
            case RocksDB:
                return initRocksDB(opts);
            case Memory:
                return initMemoryDB(opts);
            default:
                throw new UnsupportedOperationException("unsupported storage type: " + storageType);
        }
    }

    private boolean initRocksDB(final StoreEngineOptions opts) {
        StoreDBOptions rocksOpts = opts.getStoreDBOptions();
        if (rocksOpts == null) {
            rocksOpts = new StoreDBOptions();
            opts.setStoreDBOptions(rocksOpts);
        }

        String dbPath = rocksOpts.getDataPath();
        if (Strings.isNotBlank(dbPath)) {
            try {
                FileUtils.forceMkdir(new File(dbPath));
            } catch (final Throwable t) {
                LOG.error("Fail to make dir for dbPath {}.", dbPath);
                return false;
            }
        } else {
            dbPath = "";
        }
        final String dbDataPath = JRaftHelper.getDBDataPath(dbPath, this.storeId, opts.getServerAddress().getPort());
        rocksOpts.setDataPath(dbDataPath);
        this.dbPath = new File(rocksOpts.getDataPath());
        final RocksRawKVStore rocksRawKVStore = new RocksRawKVStore();
        if (!rocksRawKVStore.init(rocksOpts)) {
            LOG.error("Fail to init [RocksRawKVStore].");
            return false;
        }
        this.rawKVStore = rocksRawKVStore;
        return true;
    }

    private boolean initMemoryDB(final StoreEngineOptions opts) {
        MemoryDBOptions memoryOpts = opts.getMemoryDBOptions();
        if (memoryOpts == null) {
            memoryOpts = new MemoryDBOptions();
            opts.setMemoryDBOptions(memoryOpts);
        }
        final MemoryRawKVStore memoryRawKVStore = new MemoryRawKVStore();
        if (!memoryRawKVStore.init(memoryOpts)) {
            LOG.error("Fail to init [MemoryRawKVStore].");
            return false;
        }
        this.rawKVStore = memoryRawKVStore;
        return true;
    }

    private boolean initAllRegionEngine(final StoreEngineOptions opts, final Store store) {
        Requires.requireNonNull(opts, "opts");
        Requires.requireNonNull(store, "store");
        Requires.requireNonNull(opts.getRaftStoreOptions(), "raftDBOptions is Null");

        String baseRaftDataPath = opts.getRaftStoreOptions().getDataPath();
        if (baseRaftDataPath != null && Strings.isNotBlank(baseRaftDataPath)) {
            try {
                FileUtils.forceMkdir(new File(baseRaftDataPath));
            } catch (final Throwable t) {
                LOG.error("Fail to make dir for raftDataPath: {}.", baseRaftDataPath);
                return false;
            }
        } else {
            LOG.error("Init Region found region raft path is empty. store:{}, raftStoreOpt:{}",
                store.getId(),
                opts.getRaftStoreOptions());
            return false;
        }
        final Endpoint serverAddress = opts.getServerAddress();
        final List<RegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        final List<Region> regionList = store.getRegions();
        Requires.requireTrue(rOptsList.size() == regionList.size());
        for (int i = 0; i < rOptsList.size(); i++) {
            final RegionEngineOptions rOpts = rOptsList.get(i);
            boolean isOK = inConfiguration(rOpts.getServerAddress().toString(), rOpts.getInitialServerList());
            if (!isOK) {
                LOG.warn("Invalid serverAddress:{} not in initialServerList:{}, whole options:{}",
                    rOpts.getServerAddress(), rOpts.getInitialServerList(), rOpts);
                continue;
            }
            final Region region = regionList.get(i);
            if (Strings.isBlank(rOpts.getRaftDataPath())) {
                final String raftDataPath = JRaftHelper.getRaftDataPath(
                    baseRaftDataPath,
                    region.getId(),
                    serverAddress.getPort());
                rOpts.setRaftDataPath(raftDataPath);
            }
            Requires.requireNonNull(region.getRegionEpoch(), "regionEpoch");
            final RegionEngine engine = new RegionEngine(region, this);
            if (engine.init(rOpts)) {
                final RegionKVService regionKVService = new DefaultRegionKVService(engine);
                registerRegionKVService(regionKVService);
                this.regionEngineTable.put(region.getId(), engine);
            } else {
                LOG.error("Fail to init [RegionEngine: {}].", region);
                return false;
            }
        }
        return true;
    }

    public boolean startRegionEngine(Region region, RegionEngineOptions options) {
        options.setRaftDataPath(Paths.get(this.storeOpts.getStoreDBOptions().getDataPath(), storeId, region.getId()).toString());
        Requires.requireNonNull(region.getRegionEpoch(), "regionEpoch");
        final RegionEngine engine = new RegionEngine(region, this);
        if (engine.init(options)) {
            final RegionKVService regionKVService = new DefaultRegionKVService(engine);
            registerRegionKVService(regionKVService);
            this.regionEngineTable.put(region.getId(), engine);
            return true;
        }
        LOG.error("Fail to init [RegionEngine: {}].", region);
        return false;
    }

    private boolean inConfiguration(final String curr, final String all) {
        final PeerId currPeer = new PeerId();
        if (!currPeer.parse(curr)) {
            return false;
        }
        final Configuration allConf = new Configuration();
        if (!allConf.parse(all)) {
            return false;
        }
        return allConf.contains(currPeer) || allConf.getLearners().contains(currPeer);
    }

    private void registerRegionKVService(final RegionKVService regionKVService) {
        final RegionKVService preService = this.regionKVServiceTable.putIfAbsent(regionKVService.getRegionId(),
            regionKVService);
        if (preService != null) {
            throw new DingoRowStoreRuntimeException("RegionKVService[region=" + regionKVService.getRegionId()
                                           + "] has already been registered, can not register again!");
        }
    }

    @Override
    public String toString() {
        return "StoreEngine{storeId=" + storeId + ", startTime=" + startTime + ", dbPath=" + dbPath + ", storeOpts="
               + storeOpts + ", started=" + started + ", regions=" + getAllRegionEngines() + '}';
    }

    @Override
    public void describe(final Printer out) {
        out.println("StoreEngine:"); //
        out.print("  AllLeaderRegions:") //
            .println(getLeaderRegionIds()); //
        for (final RegionEngine r : getAllRegionEngines()) {
            r.describe(out);
        }
    }
}
