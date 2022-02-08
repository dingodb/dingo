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
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
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
import io.dingodb.store.row.options.StoreDBOptions;
import io.dingodb.store.row.storage.RawKVStore;
import io.dingodb.store.row.storage.RocksRawKVStore;
import io.dingodb.store.row.util.Strings;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ServiceLoader;
import javax.annotation.Nonnull;

import static io.dingodb.server.coordinator.config.Constants.COORDINATOR;

public class CoordinatorServer {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(CoordinatorServer.class);
    private CoordinatorContext context;
    private CoordinatorConfiguration configuration;
    private Node node;

    public CoordinatorServer() {
    }

    public CoordinatorContext context() {
        return context;
    }

    public void start() throws Exception {
        this.context = new CoordinatorContext();
        this.configuration = CoordinatorConfiguration.instance();
        this.configuration.instancePort(configuration.port());

        final String raftId = configuration.coordinatorRaftId();
        final Endpoint endpoint = new Endpoint(configuration.instanceHost(), configuration.coordinatorRaftServerPort());
        final RocksRawKVStore rawKVStore = createRocksDB();

        final CoordinatorStateMachine stateMachine = createStateMachine(raftId, rawKVStore, context);
        final Node node = RaftServiceFactory.createRaftNode(raftId, new PeerId(endpoint, 0));
        final AsyncKeyValueStore keyValueStore = createStore(rawKVStore, node);
        final ScheduleMetaAdaptor scheduleMetaAdaptor = createScheduleMetaAdaptor(keyValueStore);
        final TableMetaAdaptor tableMetaAdaptor = createTableMetaAdaptor(keyValueStore, scheduleMetaAdaptor);
        final CoordinatorMetaService metaService = createMetaService();
        final RowStoreMetaAdaptor rowStoreMetaAdaptor = createRowStoreMetaAdaptor(scheduleMetaAdaptor);


        context
            .configuration(configuration)
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
        NodeOptions nodeOptions = configuration.getAndConvert("node.options", NodeOptions.class, NodeOptions::new);
        nodeOptions.setInitialConf(new Configuration(configuration.coordinatorServers()));
        nodeOptions.setFsm(stateMachine);
        if (Strings.isBlank(nodeOptions.getLogUri())) {
            final Path logUri = Paths.get(configuration.dataDir(), COORDINATOR, "log");
            nodeOptions.setLogUri(logUri.toString());
        }
        if (Strings.isBlank(nodeOptions.getRaftMetaUri())) {
            final Path meteUri = Paths.get(configuration.dataDir(), COORDINATOR, "meta");
            nodeOptions.setRaftMetaUri(meteUri.toString());
        }
        if (Strings.isBlank(nodeOptions.getSnapshotUri())) {
            final Path snapshotUri = Paths.get(configuration.dataDir(), COORDINATOR, "snapshot");
            nodeOptions.setSnapshotUri(snapshotUri.toString());
        }
        return nodeOptions;
    }

    private TableMetaAdaptorImpl createTableMetaAdaptor(
        AsyncKeyValueStore keyValueStore,
        ScheduleMetaAdaptor metaStore) {
        return new TableMetaAdaptorImpl(keyValueStore, metaStore);
    }

    private CoordinatorMetaService createMetaService() {
        return CoordinatorMetaService.instance();
    }

    private RocksRawKVStore createRocksDB() {

        String dbPath = Paths.get(configuration.dataDir(), COORDINATOR, "db").toString();

        StoreDBOptions opts = configuration.getAndConvert(
            "cluster.coordinator.rocks",
            StoreDBOptions.class,
            StoreDBOptions::new
        );
        opts.setDataPath(dbPath);

        try {
            FileUtils.forceMkdir(new File(opts.getDataPath()));
        } catch (final Throwable t) {
            throw new RuntimeException("Fail to make dir for dbPath: " + opts.getDataPath());
        }

        RocksRawKVStore rocksRawKVStore = new RocksRawKVStore();
        if (!rocksRawKVStore.init(opts)) {
            throw new RuntimeException("Fail to init RocksRawKVStore.");
        }

        return rocksRawKVStore;
    }

}
