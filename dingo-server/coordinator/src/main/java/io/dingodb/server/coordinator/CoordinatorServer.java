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

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.util.Files;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.raft.Node;
import io.dingodb.raft.NodeManager;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.core.DefaultJRaftServiceFactory;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.kv.storage.MemoryRawKVStore;
import io.dingodb.raft.kv.storage.RawKVStore;
import io.dingodb.raft.kv.storage.RocksRawKVStore;
import io.dingodb.raft.option.NodeOptions;
import io.dingodb.raft.option.RaftLogStoreOptions;
import io.dingodb.raft.rpc.RaftRpcServerFactory;
import io.dingodb.raft.rpc.RpcServer;
import io.dingodb.raft.storage.LogStorage;
import io.dingodb.raft.storage.impl.RocksDBLogStore;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.server.api.ClusterServiceApi;
import io.dingodb.server.api.LogLevelApi;
import io.dingodb.server.coordinator.cluster.service.CoordinatorClusterService;
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
import io.dingodb.server.coordinator.meta.service.DingoMetaService;
import io.dingodb.server.coordinator.state.CoordinatorStateMachine;
import io.dingodb.server.coordinator.store.MetaStore;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ServiceLoader;

import static io.dingodb.raft.RaftServiceFactory.createRaftNode;
import static io.dingodb.server.coordinator.config.Constants.RAFT;

@Slf4j
public class CoordinatorServer {

    private CoordinatorConfiguration configuration;
    private NetService netService;

    public CoordinatorServer() {
    }

    public void start(CoordinatorConfiguration configuration) throws Exception {
        this.configuration = configuration;
        log.info("Coordinator configuration: {}.", this.configuration);
        log.info("Dingo configuration: {}.", DingoConfiguration.instance());

        Files.createDirectories(Paths.get(configuration.getDataPath()));

        RawKVStore memoryStore = new MemoryRawKVStore();
        RawKVStore rocksStore = new RocksRawKVStore(
            Paths.get(configuration.getDataPath(), "db").toString(), configuration.getRocks()
        );

        Endpoint endpoint = new Endpoint(DingoConfiguration.host(), configuration.getRaft().getPort());
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(endpoint);
        rpcServer.init(null);
        NodeManager.getInstance().addAddress(endpoint);

        Node node = createRaftNode(configuration.getRaft().getGroup(), new PeerId(endpoint, 0));
        MetaStore metaStore = new MetaStore(node, rocksStore);
        CoordinatorStateMachine stateMachine = new CoordinatorStateMachine(node, memoryStore, rocksStore, metaStore);
        NodeOptions nodeOptions = getNodeOptions();
        nodeOptions.setFsm(stateMachine);
        nodeOptions.setLogStorage(createLogStorage(nodeOptions));
        node.init(nodeOptions);

        netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
        netService.listenPort(DingoConfiguration.port());
        netService.apiRegistry().register(LogLevelApi.class, LogLevelApi.INSTANCE);
        netService.apiRegistry().register(ClusterServiceApi.class, CoordinatorClusterService.instance());

        DingoMetaService.init();

    }

    private LogStorage createLogStorage(NodeOptions nodeOptions) {
        Path logPath = Paths.get(configuration.getDataPath(), RAFT, "log");
        Files.createDirectories(logPath);
        RaftLogStoreOptions logStoreOptions = new RaftLogStoreOptions();
        logStoreOptions.setDataPath(logPath.toString());
        logStoreOptions.setLogEntryCodecFactory(DefaultJRaftServiceFactory.newInstance().createLogEntryCodecFactory());
        logStoreOptions.setRaftLogStorageOptions(configuration.getRaftLogStorageOptions());
        RocksDBLogStore logStore = new RocksDBLogStore();
        if (!logStore.init(logStoreOptions)) {
            log.error("Fail to init [RocksDBLogStore]");
        }
        return nodeOptions.getServiceFactory().createLogStorage(PrimitiveCodec.encodeInt(0), logStore);
    }

    private NodeOptions getNodeOptions() {
        NodeOptions nodeOptions = configuration.getRaft().getNode();
        if (nodeOptions == null) {
            configuration.getRaft().setNode(nodeOptions = new NodeOptions());
        }
        if (nodeOptions.getInitialConf() == null || nodeOptions.getInitialConf().isEmpty()) {
            Configuration initialConf = new Configuration();
            String svrList = configuration.getRaft().getInitRaftSvrList();
            if (!initialConf.parse(svrList)) {
                throw new RuntimeException("Raft configuration parse error, initRaftSvrList: " + svrList);
            }
            nodeOptions.setInitialConf(initialConf);
        }

        String dataPath = configuration.getDataPath();
        if (nodeOptions.getRaftMetaUri() == null || nodeOptions.getRaftMetaUri().isEmpty()) {
            Path metaPath = Paths.get(dataPath, RAFT, "meta");
            try {
                Files.createDirectories(metaPath);
            } catch (final Throwable t) {
                throw new RuntimeException("Fail to make dir for meta: " + metaPath);
            }
            nodeOptions.setRaftMetaUri(metaPath.toString());
        }
        if (nodeOptions.getSnapshotUri() == null || nodeOptions.getSnapshotUri().isEmpty()) {
            Path snapshotPath = Paths.get(dataPath, RAFT, "snapshot");
            try {
                Files.createDirectories(snapshotPath);
            } catch (final Throwable t) {
                throw new RuntimeException("Fail to make dir for snapshot: " + snapshotPath);
            }
            nodeOptions.setSnapshotUri(snapshotPath.toString());
        }
        return nodeOptions;
    }

}
