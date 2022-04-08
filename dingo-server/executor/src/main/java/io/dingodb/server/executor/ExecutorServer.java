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

import io.dingodb.common.config.DingoOptions;
import io.dingodb.exec.Services;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.raft.option.CliOptions;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.server.api.LogLevelApi;
import io.dingodb.server.executor.config.ExecutorExtOptions;
import io.dingodb.server.executor.config.ExecutorOptions;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.api.StoreServiceProvider;
import io.dingodb.store.row.RowStoreInstance;
import io.dingodb.store.row.options.DingoRowStoreOptions;
import io.dingodb.store.row.options.HeartbeatOptions;
import io.dingodb.store.row.options.PlacementDriverOptions;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

@Slf4j
public class ExecutorServer {

    private final NetService netService;

    private ExecutorOptions svrOpts;

    public ExecutorServer() {
        this.netService = loadNetService();
    }

    private NetService loadNetService() {
        NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
        Services.initNetService();
        return netService;
    }

    public void start(final ExecutorOptions opts) throws Exception {
        this.svrOpts = opts;
        log.info("Executor all configuration: {}.", this.svrOpts);
        log.info("instance configuration: {}.", DingoOptions.instance());

        netService.listenPort(svrOpts.getExchange().getPort());
        DingoRowStoreOptions rowStoreOpts = buildRowStoreOptions();
        RowStoreInstance.setRowStoreOptions(rowStoreOpts);

        StoreService storeService = loadStoreService();
        storeService.getInstance("/tmp");

        LogLevelApi logLevel = new LogLevelApi() {};
        logLevel.init();

        // todo refactor
        storeHeartBeatSender();
    }

    private DingoRowStoreOptions buildRowStoreOptions() {
        DingoRowStoreOptions rowStoreOpts = new DingoRowStoreOptions();
        ExecutorExtOptions extOpts = svrOpts.getOptions();

        rowStoreOpts.setClusterName(DingoOptions.instance().getClusterOpts().getName());
        rowStoreOpts.setInitialServerList(svrOpts.getRaft().getInitExecRaftSvrList());
        rowStoreOpts.setFailoverRetries(extOpts.getCliOptions().getMaxRetry());
        rowStoreOpts.setFutureTimeoutMillis(extOpts.getCliOptions().getTimeoutMs());

        PlacementDriverOptions driverOptions = new PlacementDriverOptions();
        driverOptions.setFake(false);
        driverOptions.setPdGroupId(extOpts.getCoordOptions().getGroup());
        driverOptions.setInitialPdServerList(extOpts.getCoordOptions().getInitCoordRaftSvrList());
        CliOptions cliOptions = new CliOptions();
        cliOptions.setMaxRetry(extOpts.getCliOptions().getMaxRetry());
        cliOptions.setTimeoutMs(extOpts.getCliOptions().getTimeoutMs());
        driverOptions.setCliOptions(cliOptions);
        rowStoreOpts.setPlacementDriverOptions(driverOptions);

        Endpoint endpoint = new Endpoint(svrOpts.getIp(), svrOpts.getRaft().getPort());
        extOpts.getStoreEngineOptions().setServerAddress(endpoint);

        rowStoreOpts.setStoreEngineOptions(extOpts.getStoreEngineOptions());
        return rowStoreOpts;
    }

    private void storeHeartBeatSender() {
        HeartbeatOptions heartbeatOpts =
            svrOpts.getOptions().getStoreEngineOptions().getHeartbeatOptions();
        if (heartbeatOpts == null) {
            heartbeatOpts = new HeartbeatOptions();
        }
        StoreHeartbeatSender sender = new StoreHeartbeatSender(RowStoreInstance.kvStore().getStoreEngine());
        sender.init(heartbeatOpts);
    }

    private StoreService loadStoreService() {
        String store = "";
        List<StoreServiceProvider> storeServiceProviders = new ArrayList<>();
        ServiceLoader.load(StoreServiceProvider.class).forEach(storeServiceProviders::add);
        if (storeServiceProviders.size() == 1) {
            return Optional.ofNullable(storeServiceProviders.get(0)).map(StoreServiceProvider::get).orElse(null);
        }
        return storeServiceProviders.stream().filter(provider -> store.equals(provider.getClass().getName())).findAny()
            .map(StoreServiceProvider::get).orElse(null);
    }

}
