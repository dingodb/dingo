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

import io.dingodb.exec.Services;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.executor.config.ExecutorConfiguration;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.api.StoreServiceProvider;
import io.dingodb.store.row.RowStoreInstance;
import io.dingodb.store.row.options.HeartbeatOptions;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

@Slf4j
public class ExecutorServer {


    private final NetService netService;

    private final ExecutorConfiguration configuration;

    public ExecutorServer() {
        this.configuration = ExecutorConfiguration.instance();
        this.configuration.instancePort(configuration.port());

        this.netService = loadNetService();
    }

    private NetService loadNetService() {
        NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
        Services.initNetService();
        return netService;
    }

    public void start() throws Exception {
        netService.listenPort(configuration.port());
        StoreService storeService = loadStoreService();
        storeService.getInstance(configuration.dataDir());

        // todo refactor
        storeHeartBeatSender();
    }

    private void storeHeartBeatSender() {
        HeartbeatOptions heartbeatOpts = RowStoreInstance.getKvStoreOptions().getStoreEngineOptions()
            .getHeartbeatOptions();
        if (heartbeatOpts == null) {
            heartbeatOpts = new HeartbeatOptions();
        }
        StoreHeartbeatSender sender = new StoreHeartbeatSender(RowStoreInstance.kvStore().getStoreEngine());
        sender.init(heartbeatOpts);
    }

    private StoreService loadStoreService() {
        String store = configuration.store();
        List<StoreServiceProvider> storeServiceProviders = new ArrayList<>();
        ServiceLoader.load(StoreServiceProvider.class).forEach(storeServiceProviders::add);
        if (storeServiceProviders.size() == 1) {
            return Optional.ofNullable(storeServiceProviders.get(0)).map(StoreServiceProvider::get).orElse(null);
        }
        return storeServiceProviders.stream().filter(provider -> store.equals(provider.getClass().getName())).findAny()
            .map(StoreServiceProvider::get).orElse(null);
    }

}
