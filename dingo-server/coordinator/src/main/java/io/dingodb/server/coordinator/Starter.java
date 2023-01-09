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
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.util.FileUtils;
import io.dingodb.mpu.core.Core;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.storage.Storage;
import io.dingodb.mpu.storage.rocks.RocksStorage;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.LogLevelApi;
import io.dingodb.server.coordinator.api.ClusterServiceApi;
import io.dingodb.server.coordinator.config.Configuration;
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
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setRole(DingoRole.COORDINATOR);
        DingoConfiguration.parse(this.config);
        Configuration configuration = Configuration.instance();
        log.info("Coordinator configuration: {}.", configuration);
        log.info("Dingo configuration: {}.", DingoConfiguration.instance());

        Path path = Paths.get(configuration.getDataPath());
        FileUtils.createDirectories(path);

        List<CoreMeta> metas = new ArrayList<>();
        List<Location> locations = Configuration.servers();
        for (int i = 0; i < locations.size(); i++) {
            Location location = locations.get(i);
            metas.add(new CoreMeta(
                CommonId.prefix(ID_TYPE.service, SERVICE_IDENTIFIER.coordinator, 1, i),
                CommonId.prefix(ID_TYPE.service, SERVICE_IDENTIFIER.coordinator, 1, 1),
                location
            ));
        }

        CoreMeta meta = metas.remove(locations.indexOf(DingoConfiguration.location()));
        Storage storage = new RocksStorage(meta.label, Paths.get(configuration.getDataPath(), "db"));
        //Storage storage = new MemStorage();
        Core core = new Core(meta, metas, storage, null);
        CoordinatorStateMachine.init(core);
        log.info("Start listenPort {}:{}", DingoConfiguration.host(), DingoConfiguration.port());
        NetService.getDefault().listenPort(DingoConfiguration.host(), DingoConfiguration.port());
        ApiRegistry.getDefault().register(LogLevelApi.class, io.dingodb.server.coordinator.api.LogLevelApi.instance());
        ApiRegistry.getDefault().register(io.dingodb.server.api.ClusterServiceApi.class, ClusterServiceApi.instance());
        core.start();
    }
}
