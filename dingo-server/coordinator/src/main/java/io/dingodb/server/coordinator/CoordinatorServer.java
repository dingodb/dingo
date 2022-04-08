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

package io.dingodb.server.coordinator;

import io.dingodb.common.config.DingoOptions;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.raft.Node;
import io.dingodb.raft.NodeManager;
import io.dingodb.raft.RaftServiceFactory;
import io.dingodb.raft.StateMachine;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.option.NodeOptions;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.server.api.LogLevelApi;
import io.dingodb.server.coordinator.cluster.service.CoordinatorClusterService;
import io.dingodb.server.coordinator.config.CoordinatorOptions;
import io.dingodb.server.coordinator.context.CoordinatorContext;
import io.dingodb.server.coordinator.meta.RowStoreMetaAdaptor;
import io.dingodb.server.coordinator.meta.ScheduleMetaAdaptor;
import io.dingodb.server.coordinator.meta.TableMetaAdaptor;
import io.dingodb.server.coordinator.meta.impl.RowStoreMetaAdaptorImpl;
import io.dingodb.server.coordinator.meta.impl.ScheduleMetaAdaptorImpl;
import io.dingodb.server.coordinator.meta.impl.TableMetaAdaptorImpl;
import io.dingodb.server.coordinator.meta.service.CoordinatorMetaService;
import io.dingodb.server.coordinator.service.LeaderFollowerServiceProvider;
import io.dingodb.server.coordinator.service.impl.CoordinatorLeaderFollowerServiceProvider;
import io.dingodb.server.coordinator.state.CoordinatorStateMachine;
import io.dingodb.server.coordinator.store.AsyncKeyValueStore;
import io.dingodb.server.coordinator.store.RaftAsyncKeyValueStore;
import io.dingodb.store.row.storage.RawKVStore;
import io.dingodb.store.row.storage.RocksRawKVStore;
import io.dingodb.store.row.util.Strings;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import java.io.File;
import java.nio.file.Paths;
import java.util.ServiceLoader;
import javax.annotation.Nonnull;

import static io.dingodb.server.coordinator.config.Constants.COORDINATOR;

public class CoordinatorServer {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(CoordinatorServer.class);
    private CoordinatorContext context;
    private CoordinatorOptions svrOpts;
    private Node node;

    public CoordinatorServer() {
    }

    public CoordinatorContext context() {
        return context;
    }

    public void start(final CoordinatorOptions opts) throws Exception {
        this.svrOpts = opts;
        log.info("Coordinator all configuration: {}.", this.svrOpts);
        log.info("instance configuration: {}.", DingoOptions.instance());

        this.context = new CoordinatorContext();

        final String raftId = svrOpts.getRaft().getGroup();
        final Endpoint endpoint = new Endpoint(svrOpts.getIp(), svrOpts.getRaft().getPort());

        final RocksRawKVStore rawKVStore = createRocksDB();

        final CoordinatorStateMachine stateMachine = createStateMachine(raftId, rawKVStore, context);
        final Node node = RaftServiceFactory.createRaftNode(raftId, new PeerId(endpoint, 0));
        final AsyncKeyValueStore keyValueStore = createStore(rawKVStore, node);
        final ScheduleMetaAdaptor scheduleMetaAdaptor = createScheduleMetaAdaptor(keyValueStore);
        final TableMetaAdaptor tableMetaAdaptor = createTableMetaAdaptor(keyValueStore, scheduleMetaAdaptor);
        final CoordinatorMetaService metaService = createMetaService();
        final RowStoreMetaAdaptor rowStoreMetaAdaptor = createRowStoreMetaAdaptor(scheduleMetaAdaptor);
        final LogLevelApi logLevel = new LogLevelApi() {};
        final CoordinatorClusterService clusterService = createClusterService();

        context
            .coordOpts(svrOpts)
            .endpoint(endpoint)
            .netService(createNetService())
            .rocksKVStore(rawKVStore)
            .stateMachine(stateMachine)
            .keyValueStore(keyValueStore)
            .node(node)
            .scheduleMetaAdaptor(scheduleMetaAdaptor)
            .serviceProvider(createServiceProvider())
            .tableMetaAdaptor(tableMetaAdaptor)
            .rowStoreMetaAdaptor(rowStoreMetaAdaptor)
            .metaService(metaService);

        NodeManager.getInstance().addAddress(endpoint);
        stateMachine.init();
        final NodeOptions nodeOptions = initNodeOptions(stateMachine);
        node.init(nodeOptions);
        keyValueStore.init();
        logLevel.init();
        clusterService.init(context);
    }

    @Nonnull
    private RowStoreMetaAdaptorImpl createRowStoreMetaAdaptor(ScheduleMetaAdaptor scheduleMetaAdaptor) {
        return new RowStoreMetaAdaptorImpl(scheduleMetaAdaptor);
    }

    private LeaderFollowerServiceProvider createServiceProvider() {
        return new CoordinatorLeaderFollowerServiceProvider();
    }

    private CoordinatorStateMachine createStateMachine(
        String coordinatorRaftId,
        RocksRawKVStore rawKVStore,
        CoordinatorContext context
    ) {
        return new CoordinatorStateMachine(coordinatorRaftId, rawKVStore, context);
    }

    private AsyncKeyValueStore createStore(RawKVStore rawKVStore, Node node) {
        return new RaftAsyncKeyValueStore(rawKVStore, node);
    }

    private ScheduleMetaAdaptor createScheduleMetaAdaptor(AsyncKeyValueStore keyValueStore) {
        return new ScheduleMetaAdaptorImpl(keyValueStore);
    }

    private NetService createNetService() {
        return ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
    }

    private NodeOptions initNodeOptions(StateMachine stateMachine) {
        final Configuration initialConf = new Configuration();
        String svrList = svrOpts.getRaft().getInitCoordRaftSvrList();
        if (!initialConf.parse(svrList)) {
            throw new RuntimeException("configuration parse error, initCoordSrvList: " + svrList);
        }

        NodeOptions nodeOpts = new NodeOptions();
        nodeOpts.setInitialConf(initialConf);
        nodeOpts.setFsm(stateMachine);

        final String dbPath = svrOpts.getOptions().getStoreDBOptions().getDataPath();
        if (Strings.isBlank(nodeOpts.getLogUri())) {
            final String logUri = Paths.get(dbPath, COORDINATOR, "log").toString();
            try {
                FileUtils.forceMkdir(new File(logUri));
                log.info("data path created: {}", logUri);
            } catch (final Throwable t) {
                throw new RuntimeException("Fail to make dir for dbPath: " + logUri);
            }
            nodeOpts.setLogUri(logUri);
        }
        if (Strings.isBlank(nodeOpts.getRaftMetaUri())) {
            final String meteUri = Paths.get(dbPath, COORDINATOR, "meta").toString();
            nodeOpts.setRaftMetaUri(meteUri);
        }
        if (Strings.isBlank(nodeOpts.getSnapshotUri())) {
            final String snapshotUri = Paths.get(dbPath, COORDINATOR, "snapshot").toString();
            nodeOpts.setSnapshotUri(snapshotUri);
        }
        return nodeOpts;
    }

    private TableMetaAdaptorImpl createTableMetaAdaptor(
        AsyncKeyValueStore keyValueStore,
        ScheduleMetaAdaptor metaStore) {
        return new TableMetaAdaptorImpl(keyValueStore, metaStore);
    }

    private CoordinatorMetaService createMetaService() {
        return CoordinatorMetaService.instance();
    }

    private CoordinatorClusterService createClusterService() {
        return CoordinatorClusterService.instance();
    }

    private RocksRawKVStore createRocksDB() {
        final String dbPath = svrOpts.getOptions().getStoreDBOptions().getDataPath();
        final String dataPath = Paths.get(dbPath, COORDINATOR, "db").toString();
        svrOpts.getOptions().getStoreDBOptions().setDataPath(dbPath);

        try {
            FileUtils.forceMkdir(new File(dataPath));
            log.info("data path created: {}", dataPath);
        } catch (final Throwable t) {
            throw new RuntimeException("Fail to make dir for dbPath: " + dataPath);
        }

        RocksRawKVStore rocksRawKVStore = new RocksRawKVStore();
        if (!rocksRawKVStore.init(svrOpts.getOptions().getStoreDBOptions())) {
            throw new RuntimeException("Fail to init RocksRawKVStore.");
        }

        return rocksRawKVStore;
    }

}
