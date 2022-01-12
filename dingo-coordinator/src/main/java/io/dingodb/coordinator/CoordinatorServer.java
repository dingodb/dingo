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

package io.dingodb.coordinator;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.util.Endpoint;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.coordinator.config.ServerConfiguration;
import io.dingodb.coordinator.context.CoordinatorContext;
import io.dingodb.coordinator.meta.impl.CoordinatorMetaService;
import io.dingodb.coordinator.meta.impl.MetaStoreImpl;
import io.dingodb.coordinator.meta.impl.TableMetaAdaptorImpl;
import io.dingodb.coordinator.service.impl.CoordinatorLeaderFollowerServiceProvider;
import io.dingodb.coordinator.state.CoordinatorStateMachine;
import io.dingodb.coordinator.store.RaftAsyncKeyValueStore;
import io.dingodb.dingokv.options.MemoryDBOptions;
import io.dingodb.dingokv.options.RocksDBOptions;
import io.dingodb.dingokv.storage.MemoryRawKVStore;
import io.dingodb.dingokv.storage.RocksRawKVStore;
import io.dingodb.dingokv.util.Strings;
import io.dingodb.meta.MetaService;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ServiceLoader;
import java.util.concurrent.ThreadPoolExecutor;
import javax.annotation.Nonnull;

@Slf4j
public class CoordinatorServer {

    private CoordinatorContext context;
    private ServerConfiguration configuration;
    private Node node;

    public CoordinatorServer() {
    }

    public CoordinatorContext context() {
        return context;
    }

    public void start() throws Exception {
        this.context = new CoordinatorContext();
        this.configuration = ServerConfiguration.instance();
        this.configuration.port(configuration.coordinatorPort());

        Endpoint endpoint = new Endpoint(configuration.instanceHost(), configuration.port());
        NodeManager.getInstance().addAddress(endpoint);

        context
            .configuration(configuration)
            .endpoint(endpoint)
            .netService(initNetService())
            .rocksKVStore(initRocksDB())
            .stateMachine(new CoordinatorStateMachine(context))
        ;

        this.node = RaftServiceFactory.createRaftNode(configuration.coordinatorRaftId(), new PeerId(endpoint, 0));

        RaftAsyncKeyValueStore keyValueStore;

        context
            .keyValueStore(keyValueStore = initStore())
            .node(this.node)
            .metaStore(initMetaAdaptor(context.keyValueStore()));


        context.serviceProvider(new CoordinatorLeaderFollowerServiceProvider(context().netService()));

        node.init(initNodeOptions(context.stateMachine()));
        keyValueStore.init(context);
        context.metaStore().init();

        context.metaAdaptor(initTableMetaAdaptor(context.keyValueStore(), (MetaStoreImpl) context.metaStore()));
        context.metaService(initMetaService(context.metaAdaptor()));
    }

    @Nonnull
    private RaftAsyncKeyValueStore initStore() {
        return new RaftAsyncKeyValueStore();
    }

    private MetaStoreImpl initMetaAdaptor(RaftAsyncKeyValueStore keyValueStore) {
        return new MetaStoreImpl(keyValueStore);
    }

    private NetService initNetService() {
        return ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
    }

    private NodeOptions initNodeOptions(StateMachine stateMachine) {
        NodeOptions nodeOptions = configuration.getAndConvert("node.options", NodeOptions.class, NodeOptions::new);
        nodeOptions.setInitialConf(new Configuration(configuration.coordinatorServers()));
        nodeOptions.setFsm(stateMachine);
        if (Strings.isBlank(nodeOptions.getLogUri())) {
            final Path logUri = Paths.get(configuration.dataDir(), "log");
            nodeOptions.setLogUri(logUri.toString());
        }
        if (Strings.isBlank(nodeOptions.getRaftMetaUri())) {
            final Path meteUri = Paths.get(configuration.dataDir(), "meta");
            nodeOptions.setRaftMetaUri(meteUri.toString());
        }
        if (Strings.isBlank(nodeOptions.getSnapshotUri())) {
            final Path snapshotUri = Paths.get(configuration.dataDir(), "snapshot");
            nodeOptions.setSnapshotUri(snapshotUri.toString());
        }
        return nodeOptions;
    }

    private TableMetaAdaptorImpl initTableMetaAdaptor(RaftAsyncKeyValueStore keyValueStore, MetaStoreImpl metaStore) {
        return new TableMetaAdaptorImpl(keyValueStore, metaStore);
    }

    private MetaService initMetaService(TableMetaAdaptorImpl metaAdaptor) {
        CoordinatorMetaService metaService = CoordinatorMetaService.instance();
        metaService.init(metaAdaptor);
        return metaService;
    }

    private RocksRawKVStore initRocksDB() {

        String dbPath = Paths.get(configuration.dataDir(), "db").toString();

        RocksDBOptions opts = configuration.getAndConvert(
            "cluster.coordinator.rocks",
            RocksDBOptions.class,
            RocksDBOptions::new
        );
        opts.setDbPath(dbPath);

        try {
            FileUtils.forceMkdir(new File(opts.getDbPath()));
        } catch (final Throwable t) {
            throw new RuntimeException("Fail to make dir for dbPath: " + opts.getDbPath());
        }

        RocksRawKVStore rocksRawKVStore = new RocksRawKVStore();
        if (!rocksRawKVStore.init(opts)) {
            throw new RuntimeException("Fail to init [RocksRawKVStore].");
        }

        return rocksRawKVStore;
    }

    private ThreadPoolExecutor raftExecutor() {
        return new ThreadPoolBuilder()
            .name("Raft")
            .build();
    }

    private ThreadPoolExecutor cliExecutor() {
        return new ThreadPoolBuilder()
            .name("Cli-")
            .build();
    }

    private MemoryRawKVStore initMemoryDB(MemoryDBOptions opts) {
        MemoryRawKVStore memoryRawKVStore = new MemoryRawKVStore();
        if (!memoryRawKVStore.init(opts)) {
            throw new RuntimeException("Fail to init [MemoryRawKVStore].");
        }
        return memoryRawKVStore;
    }

}
