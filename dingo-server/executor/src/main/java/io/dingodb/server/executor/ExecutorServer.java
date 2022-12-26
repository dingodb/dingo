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
import io.dingodb.common.Executive;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.NoBreakFunctions;
import io.dingodb.exec.Services;
import io.dingodb.mpu.instruction.InstructionSetRegistry;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.api.Ping;
import io.dingodb.server.ExecutiveRegistry;
import io.dingodb.server.api.LogLevelApi;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.api.ServerApi;
import io.dingodb.server.api.ServiceConnectApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.client.reload.ReloadHandler;
import io.dingodb.server.executor.api.DriverProxyApi;
import io.dingodb.server.executor.api.ExecutorApi;
import io.dingodb.server.executor.api.TableApi;
import io.dingodb.server.executor.config.Configuration;
import io.dingodb.server.executor.sidebar.TableInstructions;
import io.dingodb.server.executor.sidebar.TableSidebar;
import io.dingodb.server.executor.store.LocalMetaStore;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.api.StoreServiceProvider;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class ExecutorServer {

    private NetService netService;
    private StoreService storeService;

    private StoreInstance storeInstance;

    private CoordinatorConnector coordinatorConnector;
    private CommonId id;

    private MetaServiceApi metaServiceApi;

    private TableApi tableApi;
    private DriverProxyApi driverProxyApi;

    private ExecutorApi executorApi;
    private ServerApi serverApi;

    private LocalMetaStore store;

    public ExecutorServer() {
        this.netService = loadNetService();
        this.storeService = loadStoreService();
        this.coordinatorConnector = CoordinatorConnector.getDefault();
        this.serverApi = netService.apiRegistry().proxy(ServerApi.class, coordinatorConnector);
        this.metaServiceApi = netService.apiRegistry().proxy(MetaServiceApi.class, coordinatorConnector);

    }

    public void start() throws Exception {
        log.info("Starting executor......");
        initId();
        DingoConfiguration.instance().setServerId(this.id);
        log.info("Start listenPort {}:{}", DingoConfiguration.host(), DingoConfiguration.port());
        netService.listenPort(DingoConfiguration.host(), DingoConfiguration.port());
        initStore();
        initAllApi();
        loadExecutive();
        InstructionSetRegistry.register(TableInstructions.id, TableInstructions.INSTANCE);
        Map<CommonId, TableDefinition> tables = store.getTables();
        tables.entrySet().forEach(NoBreakFunctions.wrap(
            e -> {
                TableSidebar tableSidebar = TableSidebar.create(
                    e.getKey(), store.getTableMirrors(e.getKey()), e.getValue()
                );
                tableSidebar.start();
                tableApi.register(tableSidebar);
            }
        ));
        ReloadHandler.handler.registryReloadChannel();
        log.info("Starting executor success.");
        //TableDefinition definition = Table.DEFINITION;
        //definition.addIndex(new Index("name", new String[] {"name"}, false));
        //CommonId tableId = new CommonId((byte) 'T', new byte[] {'T', 'B'}, 1, 1);
        //tableApi.createTable(tableId, definition, Collections.emptyMap());
        //System.out.println(ServiceConnectApi.INSTANCE.leader(tableId));
        //System.out.println(tableApi.partitions(tableId));
        //System.out.println(ServiceConnectApi.INSTANCE.leader(tableApi.partitions(tableId).get(0).getId()));
        //System.out.println(storeService.getInstance(tableId).exist("key".getBytes()));
        //storeService.getInstance(tableId).upsertKeyValue("key".getBytes(), "value".getBytes());
        //System.out.println(new String(storeService.getInstance(tableId).getValueByPrimaryKey("key".getBytes())));
    }

    private void initId() throws IOException {
        String dataPath = Configuration.dataPath();
        Path path = Paths.get(dataPath);
        Path idPath = Paths.get(dataPath, "id");
        log.info("Get server id, path: {}", idPath);
        if (Files.isDirectory(path) && Files.exists(idPath)) {
            this.id = CommonId.decode(Files.readAllBytes(idPath));
            log.info("Executor start, id: {}", id);
        } else {
            Files.createDirectories(path);
            while (true) {
                try {
                    Ping.ping(coordinatorConnector.get());
                    this.id = serverApi.registerExecutor(Executor.builder()
                        .host(DingoConfiguration.host())
                        .port(DingoConfiguration.port())
                        .processors(Runtime.getRuntime().availableProcessors())
                        .memory(Runtime.getRuntime().maxMemory())
                        .build());
                    break;
                } catch (Exception ignored) {
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(3));
                }
            }
            Files.write(idPath, this.id.encode());
            log.info("New executor, id: {}", id);
        }
    }

    private void initStore() throws Exception {
        Map<String, Object> storeServiceConfig = new HashMap<>();
        storeServiceConfig.put("MetaServiceApi", metaServiceApi);
        storeService.addConfiguration(storeServiceConfig);
        store = LocalMetaStore.INSTANCE;

    }

    private void initAllApi() {
        tableApi = TableApi.INSTANCE;
        driverProxyApi = new DriverProxyApi(netService);
        executorApi = new ExecutorApi(netService, storeService);
        ApiRegistry.getDefault().register(LogLevelApi.class, io.dingodb.server.executor.api.LogLevelApi.instance());
        ApiRegistry.getDefault().register(ServiceConnectApi.class, ServiceConnectApi.INSTANCE);
        ApiRegistry.getDefault().register(io.dingodb.server.api.TableApi.class, tableApi);
    }

    private NetService loadNetService() {
        NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
        Services.initNetService();
        return netService;
    }

    private StoreService loadStoreService() {

        return ServiceLoader.load(StoreServiceProvider.class).iterator().next().get();
    }

    private void loadExecutive() {
        ServiceLoader.load(Executive.class).iterator().forEachRemaining(ExecutiveRegistry::register);
    }

}
