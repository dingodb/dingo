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
import io.dingodb.common.config.ClOptions;
import io.dingodb.common.config.ClusterOptions;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.config.DingoOptions;
import io.dingodb.common.config.ExchangeOptions;
import io.dingodb.server.coordinator.config.CoordinatorExtOptions;
import io.dingodb.server.coordinator.config.CoordinatorOptions;
import io.dingodb.server.coordinator.config.CoordinatorRaftOptions;
import io.dingodb.server.coordinator.config.CoordinatorSchedule;
import io.dingodb.store.row.options.StoreDBOptions;
import lombok.extern.slf4j.Slf4j;

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

        DingoConfiguration.configParse(this.config);
        CoordinatorOptions svrOpts = DingoConfiguration.instance().getAndConvert("coordinator",
            CoordinatorOptions.class, CoordinatorOptions::new);

        ClusterOptions clusterOpts = DingoConfiguration.instance().getAndConvert("cluster",
            ClusterOptions.class, ClusterOptions::new);

        DingoOptions.instance().setClusterOpts(clusterOpts);
        DingoOptions.instance().setIp(svrOpts.getIp());
        DingoOptions.instance().setExchange(svrOpts.getExchange());
        DingoOptions.instance().setCliOptions(svrOpts.getOptions().getCliOptions());
        DingoOptions.instance().setQueueCapacity(svrOpts.getOptions().getCapacity());

        CoordinatorServer server = new CoordinatorServer();
        server.start(svrOpts);
    }
}
