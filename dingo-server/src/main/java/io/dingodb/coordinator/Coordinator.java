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

import io.dingodb.coordinator.service.CoordinatorLeaderFollowerServiceProvider;
import io.dingodb.helix.part.HelixPart;
import io.dingodb.helix.part.impl.ZkHelixControllerPart;
import io.dingodb.helix.part.impl.ZkHelixParticipantPart;
import io.dingodb.helix.state.LeaderFollowerStateModel;
import io.dingodb.meta.helix.HelixMetaService;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.ServerConfiguration;
import io.dingodb.server.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.model.InstanceConfig;

import java.util.ServiceLoader;

@Slf4j
public class Coordinator {

    private final ServerConfiguration configuration;
    private final HelixPart coordinatorHelixPart;
    private final HelixPart controllerHelixPart;
    private final HelixMetaService metaService;
    private final NetService netService;

    public Coordinator() {
        this.configuration = ServerConfiguration.instance();
        this.configuration.port(configuration.coordinatorPort());
        netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
        metaService = HelixMetaService.instance();

        coordinatorHelixPart = new ZkHelixParticipantPart<>(
            LeaderFollowerStateModel.Factory::new,
            new CoordinatorLeaderFollowerServiceProvider(netService),
            configuration
        );

        controllerHelixPart = new ZkHelixControllerPart(
            configuration
        );

    }

    public void init() throws Exception {
        controllerHelixPart.init();
        coordinatorHelixPart.init();
    }

    public void start() throws Exception {
        controllerHelixPart.start();
        coordinatorHelixPart.start();

        addTagIfNeed(coordinatorHelixPart, configuration.coordinatorTag());
        metaService.init(coordinatorHelixPart);
        netService.listenPort(configuration.port());
        //Services.initNetService();
        netService.registerMessageListenerProvider(SqlExecutor.SQL_EXECUTOR_TAG, new SqlExecutor.SqlExecutorProvider());

    }

    private void addTagIfNeed(HelixPart helixPart, String tag) {
        String instanceName = helixPart.name();
        String clusterName = helixPart.clusterName();
        InstanceConfig instanceConfig = helixPart.helixAccessor().getInstanceConfig(clusterName, instanceName);
        if (instanceConfig == null) {
            instanceConfig = new InstanceConfig(instanceName);
        }
        if (!instanceConfig.containsTag(tag)) {
            log.info("Controller: {} doesn't contain group tag: {}. Adding one.", instanceName, tag);
            instanceConfig.addTag(tag);
            helixPart.helixAccessor().updateInstanceConfig(clusterName, instanceName, instanceConfig);
        }
    }

    public void stop() {
        try {
            coordinatorHelixPart.close();
            controllerHelixPart.close();
        } catch (Exception e) {
            log.error("Stop error.", e);
        }
    }

    public ServerConfiguration configuration() {
        return configuration;
    }

    public HelixPart coordinatorHelixPart() {
        return coordinatorHelixPart;
    }

    public HelixPart controllerHelixPart() {
        return controllerHelixPart;
    }
}
