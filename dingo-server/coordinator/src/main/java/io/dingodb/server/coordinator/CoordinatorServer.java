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
import io.dingodb.raft.storage.LogStore;
import io.dingodb.raft.storage.impl.RocksDBLogStore;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.server.api.LogLevelApi;
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
import io.dingodb.server.coordinator.meta.service.DingoMetaService;
import io.dingodb.server.coordinator.state.CoordinatorStateMachine;
import io.dingodb.server.coordinator.store.MetaStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
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

        if (configuration.getLogDataPath() == null || configuration.getLogDataPath().isEmpty()) {
            configuration.setLogDataPath(Paths.get(configuration.getDataPath(),  "log").toString());
            configuration.setDataPath(Paths.get(configuration.getDataPath(),"db").toString());
        }
        try {
            FileUtils.forceMkdir(new File(configuration.getDataPath()));
        } catch (final Throwable t) {
            log.error("Fail to make dir for dataPath {}.", configuration.getDataPath());
        }
        try {
            FileUtils.forceMkdir(new File(configuration.getLogDataPath()));
        } catch (final Throwable t) {
            log.error("Fail to make dir for logDataPath {}.", configuration.getLogDataPath());
        }
        final RawKVStore memoryStore = new MemoryRawKVStore();
        final RawKVStore rocksStore = new RocksRawKVStore(configuration.getDataPath(), configuration.getRocks());
        final LogStore logStore = new RocksDBLogStore();
        RaftLogStoreOptions logStoreOptions = new RaftLogStoreOptions();
        logStoreOptions.setDataPath(configuration.getLogDataPath());
        logStoreOptions.setLogEntryCodecFactory(DefaultJRaftServiceFactory.newInstance().createLogEntryCodecFactory());
        logStoreOptions.setRaftLogStorageOptions(configuration.getRaftLogStorageOptions());
        if (logStore.init(logStoreOptions)) {
            log.error("Fail to init [RocksDBLogStore]");
        }

        Endpoint endpoint = new Endpoint(DingoConfiguration.host(), configuration.getRaft().getPort());
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(endpoint);
        rpcServer.init(null);
        NodeManager.getInstance().addAddress(endpoint);

        Node node = createRaftNode(configuration.getRaft().getGroup(), new PeerId(endpoint, 0));
        MetaStore metaStore = new MetaStore(node, rocksStore);
        CoordinatorStateMachine stateMachine = new CoordinatorStateMachine(node, memoryStore, rocksStore, metaStore);
        NodeOptions nodeOptions = getNodeOptions();
        nodeOptions.setFsm(stateMachine);
        nodeOptions.setLogStorage(nodeOptions.getServiceFactory().createLogStorage("coordinator", logStore));
        node.init(nodeOptions);

        netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
        netService.listenPort(DingoConfiguration.port());
        netService.apiRegistry().register(LogLevelApi.class, LogLevelApi.INSTANCE);

        DingoMetaService.init();
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
