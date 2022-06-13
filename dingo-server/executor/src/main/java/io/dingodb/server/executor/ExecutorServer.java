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

package io.dingodb.server.executor;

import io.dingodb.common.CommonId;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.store.Part;
import io.dingodb.exec.Services;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.api.LogLevelApi;
import io.dingodb.server.api.ServerApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.executor.api.DriverProxyApi;
import io.dingodb.server.executor.api.ExecutorApi;
import io.dingodb.server.executor.api.TableStoreApi;
import io.dingodb.server.executor.config.ExecutorConfiguration;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.api.StoreServiceProvider;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.ServiceLoader;

@Slf4j
public class ExecutorServer {

    private NetService netService;
    private StoreService storeService;

    private StoreInstance storeInstance;

    private CoordinatorConnector coordinatorConnector;
    private CommonId id;

    private TableStoreApi tableStoreApi;
    private DriverProxyApi driverProxyApi;

    private ExecutorApi executorApi;
    private ServerApi serverApi;

    public ExecutorServer() {
        this.netService = loadNetService();
        this.storeService = loadStoreService();
        this.coordinatorConnector = CoordinatorConnector.defaultConnector();
        this.serverApi = netService.apiRegistry().proxy(ServerApi.class, coordinatorConnector);
    }

    public void start() throws Exception {
        log.info("Starting executor......");
        initId();
        DingoConfiguration.instance().setServerId(this.id);
        netService.listenPort(DingoConfiguration.port());
        initAllApi();
        initStore();
        log.info("Starting executor success.");
    }

    private void initId() throws IOException {
        String dataPath = ExecutorConfiguration.dataPath();
        Path path = Paths.get(dataPath);
        Path idPath = Paths.get(dataPath, "id");
        log.info("Get server id, path: {}", idPath);
        if (Files.isDirectory(path) && Files.exists(idPath)) {
            this.id = CommonId.decode(Files.readAllBytes(idPath));
            log.info("Executor start, id: {}", id);
        } else {
            Files.createDirectories(path);
            this.id = serverApi.registerExecutor(Executor.builder()
                .host(DingoConfiguration.host())
                .port(DingoConfiguration.port())
                .raftPort(DingoConfiguration.instance().getRaftPort())
                .processors(Runtime.getRuntime().availableProcessors())
                .memory(Runtime.getRuntime().maxMemory())
                .build());
            Files.write(idPath, this.id.encode());
            log.info("New executor, id: {}", id);
        }
    }

    private void initStore() {
        List<Part> parts = serverApi.storeMap(this.id);
        this.storeInstance = storeService.getInstance(this.id);
        log.info("Init store, parts: {}", parts);
        parts.forEach(tableStoreApi::assignTablePart);
    }

    private void initAllApi() {
        tableStoreApi = new TableStoreApi(netService, storeService);
        driverProxyApi = new DriverProxyApi(netService);
        executorApi = new ExecutorApi(netService, storeService);
        netService.apiRegistry().register(LogLevelApi.class, LogLevelApi.INSTANCE);
    }

    private NetService loadNetService() {
        NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
        Services.initNetService();
        return netService;
    }

    private StoreService loadStoreService() {
        return ServiceLoader.load(StoreServiceProvider.class).iterator().next().get();
    }

}
