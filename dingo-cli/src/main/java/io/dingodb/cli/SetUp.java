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

import java.io.FileInputStream;
import java.util.Map;

import static io.dingodb.expr.json.runtime.Parser.YAML;

public class SetUp {

    @Parameter(names = "--help", help = true, order = 0)
    private boolean help;

    @Parameter(description = "command", order = 1, required = true)
    private String cmd;

    @Parameter(names = "--config", description = "Config file path.", order = 3, required = true)
    private String config;

    public static void main(String[] args) throws Exception {
        SetUp setUp = new SetUp();
        JCommander commander = new JCommander(setUp);
        commander.parse(args);
        setUp.exec(commander);
    }

    public void exec(JCommander commander) throws Exception {
        if (help) {
            commander.usage();
            return;
        }
        DingoConfiguration configuration = DingoConfiguration.instance();
        YAML.parse(new FileInputStream(this.config), Map.class).forEach((k, v) -> configuration.set(k.toString(), v));
        switch (cmd.toUpperCase()) {
            case "CLUSTER":
                SetupClusterHandler.handler();
                break;
            case "EXECUTOR":
                SetupExecutorHandler.handler();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + cmd);
        }
    }

}
