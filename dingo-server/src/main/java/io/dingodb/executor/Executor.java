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
import io.dingodb.executor.service.ExecutorLeaderFollowerServiceProvider;
import io.dingodb.helix.HelixAccessor;
import io.dingodb.helix.part.HelixPart;
import io.dingodb.helix.part.impl.ZkHelixParticipantPart;
import io.dingodb.helix.state.LeaderFollowerStateModel;
import io.dingodb.kvstore.KvStoreService;
import io.dingodb.kvstore.KvStoreServiceProvider;
import io.dingodb.meta.helix.HelixMetaService;
import io.dingodb.net.Location;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.Constants;
import io.dingodb.server.ServerConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

@Slf4j
public class Executor {

    private final HelixPart participantHelixPart;

    private final NetService netService;
    private final HelixMetaService metaService;
    private final KvStoreService storeService;

    private final Location location;
    private final ServerConfiguration configuration;

    public Executor() {
        this.configuration = ServerConfiguration.instance();
        this.configuration.port(configuration.executorPort());
        this.location = new Location(configuration.instanceHost(), configuration.port(), configuration.dataDir());

        netService = loadNetService();
        this.storeService = loadStoreService();
        this.metaService = HelixMetaService.instance();
        participantHelixPart = new ZkHelixParticipantPart<>(
            LeaderFollowerStateModel.Factory::new,
            new ExecutorLeaderFollowerServiceProvider(netService, storeService.getInstance(location.getPath())),
            configuration
        );
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

    public void init() throws Exception {
        participantHelixPart.init();
    }

    public void start() throws Exception {
        participantHelixPart.start();
        configPath();

        metaService.init(participantHelixPart);
        netService.listenPort(configuration.port());

        Services.initNetService();
    }

    private void configPath() {
        HelixAccessor helixAccessor = participantHelixPart().helixAccessor();
        List<String> paths = Arrays.asList(configuration.dataDir().split(","));
        helixAccessor.addAllToList(helixAccessor.instanceConfig(participantHelixPart.name()), Constants.PATHS, paths);
    }

    public void close() {
        try {
            participantHelixPart.close();
        } catch (Exception e) {
            log.error("Stop error.", e);
        }
    }

    public HelixPart participantHelixPart() {
        return participantHelixPart;
    }
}
