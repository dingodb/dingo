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

package io.dingodb.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dingodb.cli.handler.SetupClusterHandler;
import io.dingodb.cli.handler.SetupExecutorHandler;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.coordinator.Coordinator;
import io.dingodb.executor.Executor;
import io.dingodb.server.ServerConfiguration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;

import java.io.FileInputStream;
import java.util.Map;

import static io.dingodb.expr.json.runtime.Parser.YAML;

public class Starter {

    @Parameter(names = "--help", description = "Print usage.", help = true, order = 0)
    private boolean help;

    @Parameter(description = "coordinator or executor", required = true, order = 1)
    private String program;

    @Parameter(names = "--config", description = "Config file path.", order = 2, required = true)
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
        ServerConfiguration configuration = ServerConfiguration.instance();
        YAML.parse(new FileInputStream(this.config), Map.class).forEach((k, v) -> configuration.set(k.toString(), v));
        ZKHelixAdmin helixAdmin = new ZKHelixAdmin.Builder().setZkAddress(configuration.zkServers()).build();
        if (!checkCluster(helixAdmin, configuration)) {
            SetupClusterHandler.handler();
        }
        switch (program.toUpperCase()) {
            case "COORDINATOR":
                Coordinator coordinator = new Coordinator();
                coordinator.init();
                coordinator.start();
                break;
            case "EXECUTOR":
                if (!checkExecutor(helixAdmin, configuration)) {
                    SetupExecutorHandler.handler();
                }
                Executor executor = new Executor();
                executor.init();
                executor.start();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + program.toUpperCase());
        }
    }

    private boolean checkExecutor(ZKHelixAdmin helixAdmin, ServerConfiguration configuration) {
        return helixAdmin
            .getInstancesInCluster(configuration.clusterName())
            .contains(configuration.instanceHost() + "_" + configuration.executorPort());
    }

    private boolean checkCluster(ZKHelixAdmin helixAdmin, ServerConfiguration configuration) {
        return helixAdmin
            .getClusters()
            .contains(configuration.clusterName());
    }

}
