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

import io.dingodb.common.CommonId;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.util.Files;
import io.dingodb.raft.Node;
import io.dingodb.raft.NodeManager;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.core.DefaultJRaftServiceFactory;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.kv.storage.MemoryRawKVStore;
import io.dingodb.raft.kv.storage.RocksRawKVStore;
import io.dingodb.raft.option.NodeOptions;
import io.dingodb.raft.option.RaftLogStorageOptions;
import io.dingodb.raft.option.RaftLogStoreOptions;
import io.dingodb.raft.storage.LogStorage;
import io.dingodb.raft.storage.impl.RocksDBLogStorage;
import io.dingodb.raft.storage.impl.RocksDBLogStore;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
import io.dingodb.server.coordinator.fake.FakeTableStoreApi;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.adaptor.impl.BaseAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.BaseStatsAdaptor;
import io.dingodb.server.coordinator.schedule.ClusterScheduler;
import io.dingodb.server.coordinator.state.CoordinatorStateMachine;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.Executor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static io.dingodb.raft.RaftServiceFactory.createRaftNode;
import static io.dingodb.server.coordinator.config.Constants.RAFT;

@Slf4j
public class TestBase {

    protected static Node node;
    protected static RocksRawKVStore store;
    protected static CoordinatorConfiguration configuration;
    protected static MetaStore metaStore;
    protected static NodeOptions nodeOptions;
    protected static CoordinatorStateMachine stateMachine;

    protected static CommonId executorId;

    protected static AtomicBoolean using = new AtomicBoolean(false);

    public static void beforeAll(Path dataPath) throws Exception {
        if (!using.compareAndSet(false, true)) {
            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(3));
            return;
        }
        DingoConfiguration.parse(TestBase.class.getResource("/coordinator.yaml").getPath());
        configuration = CoordinatorConfiguration.instance();
        Files.createDirectories(dataPath);
        store = new RocksRawKVStore(dataPath.toString(), configuration.getRocks());
        Endpoint endpoint = new Endpoint(DingoConfiguration.host(), configuration.getRaft().getPort());
        node = createRaftNode(configuration.getRaft().getGroup(), new PeerId(endpoint, 0));
        metaStore = new MetaStore(node, store);
        nodeOptions = nodeOptions(dataPath.toString());
        stateMachine = new CoordinatorStateMachine(node, new MemoryRawKVStore(), store, metaStore);
        nodeOptions.setFsm(stateMachine);
        NodeManager.getInstance().addAddress(endpoint);
        node.init(nodeOptions);
        while (!stateMachine.isLeader() || MetaAdaptorRegistry.getMetaAdaptor(Executor.class) == null) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        executorId = MetaAdaptorRegistry.getMetaAdaptor(Executor.class).save(Executor.builder()
            .host(DingoConfiguration.host())
            .port(DingoConfiguration.port())
            .build());
        DingoConfiguration.instance().setServerId(executorId);
        new FakeTableStoreApi();
    }

    public static void afterAll(Path dataPath) throws Exception {
        Files.deleteIfExists(dataPath);
    }

    private static NodeOptions nodeOptions(String dataPath) {
        NodeOptions nodeOptions = new NodeOptions();
        configuration.getRaft().setNode(nodeOptions);
        Configuration initialConf = new Configuration();
        initialConf.parse(configuration.getRaft().getInitRaftSvrList());
        nodeOptions.setInitialConf(initialConf);

        Path path = Paths.get(dataPath, RAFT, "log");
        Files.createDirectories(path);
        final RocksDBLogStore logStore = new RocksDBLogStore();
        RaftLogStoreOptions logStoreOptions = new RaftLogStoreOptions();
        logStoreOptions.setDataPath(path.toString());
        logStoreOptions.setRaftLogStorageOptions(new RaftLogStorageOptions());
        logStoreOptions.setLogEntryCodecFactory(DefaultJRaftServiceFactory.newInstance().createLogEntryCodecFactory());
        logStore.init(logStoreOptions);
        LogStorage logStorage = new RocksDBLogStorage("coordinatortest".getBytes(StandardCharsets.UTF_8), logStore);
        nodeOptions.setLogStorage(logStorage);
        path = Paths.get(dataPath, RAFT, "meta");
        Files.createDirectories(path);
        nodeOptions.setRaftMetaUri(path.toString());

        path = Paths.get(dataPath, RAFT, "snapshot");
        Files.createDirectories(path);
        nodeOptions.setSnapshotUri(path.toString());

        return nodeOptions;
    }

}
