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

package io.dingodb.store.raft;

import io.dingodb.common.CommonId;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.util.Files;
import io.dingodb.common.util.Optional;
import io.dingodb.raft.rpc.RaftRpcServerFactory;
import io.dingodb.raft.rpc.RpcServer;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.raft.config.StoreConfiguration;
import org.rocksdb.RocksDB;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

public class RaftStoreService implements StoreService {
    public static final RaftStoreService INSTANCE = new RaftStoreService();

    private final Path path = Paths.get(StoreConfiguration.dbPath());

    static {
        RocksDB.loadLibrary();
    }

    private final Map<CommonId, RaftStoreInstance> storeInstanceMap = new ConcurrentHashMap<>();

    private RaftStoreService() {
        Files.createDirectories(path);
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(
            new Endpoint(DingoConfiguration.host(), StoreConfiguration.raft().getPort()));
        rpcServer.init(null);
        PartReadWriteCollector.instance().register();
    }


    @Override
    public String name() {
        return "raft";
    }

    @Override
    public StoreInstance getInstance(@Nonnull CommonId id) {
        Path instancePath = Paths.get(StoreConfiguration.dbPath(), id.toString());
        Files.createDirectories(instancePath);
        return storeInstanceMap.compute(id, (l, i) -> i == null ? new RaftStoreInstance(instancePath, id) : i);
    }

    @Override
    public void deleteInstance(CommonId id) {
        Optional.ofNullable(storeInstanceMap.remove(id)).ifPresent(RaftStoreInstance::clear);
    }
}
