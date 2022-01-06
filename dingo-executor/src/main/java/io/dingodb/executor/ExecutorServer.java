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

package io.dingodb.executor;

import io.dingodb.exec.Services;
import io.dingodb.kvstore.KvStoreService;
import io.dingodb.kvstore.KvStoreServiceProvider;
import io.dingodb.net.Location;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

@Slf4j
public class ExecutorServer {


    private final NetService netService;
    private final KvStoreService storeService;

    private final Location location;
    private final ServerConfiguration configuration;

    public ExecutorServer() {
        this.configuration = ServerConfiguration.instance();
        this.configuration.port(configuration.executorPort());
        this.location = new Location(configuration.instanceHost(), configuration.port(), configuration.dataDir());

        netService = loadNetService();
        this.storeService = loadStoreService();
    }

    private NetService loadNetService() {
        return ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
    }

    private KvStoreService loadStoreService() {
        String store = configuration.store();
        List<KvStoreServiceProvider> storeServiceProviders = new ArrayList<>();
        ServiceLoader.load(KvStoreServiceProvider.class).forEach(storeServiceProviders::add);
        if (storeServiceProviders.size() == 1) {
            return Optional.ofNullable(storeServiceProviders.get(0)).map(KvStoreServiceProvider::get).orElse(null);
        }
        return storeServiceProviders.stream().filter(provider -> store.equals(provider.getClass().getName())).findAny()
            .map(KvStoreServiceProvider::get).orElse(null);
    }

    public void start() throws Exception {

        netService.listenPort(configuration.port());

        Services.initNetService();
    }

    public static void main(String[] args) throws Exception {
        new ExecutorServer().start();
    }
}
