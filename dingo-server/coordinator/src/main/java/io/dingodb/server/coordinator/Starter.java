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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.util.FileUtils;
import io.dingodb.mpu.core.Core;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.core.MirrorProcessingUnit;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.ClusterServiceApi;
import io.dingodb.server.api.LogLevelApi;
import io.dingodb.server.coordinator.cluster.service.CoordinatorClusterService;
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
import io.dingodb.server.coordinator.state.CoordinatorStateMachine;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.SERVICE_IDENTIFIER;

@Slf4j
public class Starter {

    @Parameter(names = "--help", description = "Print usage.", help = true, order = 0)
    private boolean help;

    @Parameter(names = "--config", description = "Config file path.", order = 1, required = true)
    private String config;

    public static void main(String[] args) throws Exception {
        Starter starter = new Starter();
        JCommander commander = JCommander.newBuilder().addObject(starter).build();
        commander.parse(args);
        starter.exec(commander);
    }

    public void exec(JCommander commander) throws Exception {
        if (help) {
            commander.usage();
            return;
        }

        DingoConfiguration.parse(this.config);
        CoordinatorConfiguration configuration = CoordinatorConfiguration.instance();
        log.info("Coordinator configuration: {}.", configuration);
        log.info("Dingo configuration: {}.", DingoConfiguration.instance());

        Path path = Paths.get(configuration.getDataPath());
        FileUtils.createDirectories(path);

        MirrorProcessingUnit mpu = new MirrorProcessingUnit(
            CommonId.prefix(ID_TYPE.service, SERVICE_IDENTIFIER.coordinator),
            Paths.get(configuration.getDataPath(), "db"),
            configuration.getDbRocksOptionsFile(),
            configuration.getLogRocksOptionsFile(),
            0
        );

        List<CoreMeta> metas = new ArrayList<>();
        List<Location> locations = CoordinatorConfiguration.servers();
        for (int i = 0; i < locations.size(); i++) {
            Location location = locations.get(i);
            metas.add(new CoreMeta(
                CommonId.prefix(ID_TYPE.service, SERVICE_IDENTIFIER.coordinator, 1, i),
                CommonId.prefix(ID_TYPE.service, SERVICE_IDENTIFIER.coordinator, 0, 1),
                mpu.id, location,
                0
            ));
        }

        Core core = mpu.createCore(locations.indexOf(DingoConfiguration.location()), metas);
        CoordinatorStateMachine.init(core);
        log.info("Start listenPort {}:{}", DingoConfiguration.host(), DingoConfiguration.port());
        NetService.getDefault().listenPort(DingoConfiguration.host(), DingoConfiguration.port());
        ApiRegistry.getDefault().register(LogLevelApi.class, LogLevelApi.INSTANCE);
        ApiRegistry.getDefault().register(ClusterServiceApi.class, CoordinatorClusterService.instance());
        core.start();
    }
}
