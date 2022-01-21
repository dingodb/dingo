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
import io.dingodb.cli.handler.SqlLineHandler;
import io.dingodb.common.config.DingoConfiguration;

import java.io.FileInputStream;
import java.util.Map;

import static io.dingodb.expr.json.runtime.Parser.YAML;

public class Tools {

    @Parameter(names = "--help", help = true, order = 0)
    private boolean help;

    @Parameter(description = "command", order = 1, required = true)
    private String cmd;

    @Parameter(names = "--config", description = "Config file path.", order = 2)
    private String config;

    @Parameter(names = "--resource", description = "Resource name.", order = 3)
        private String resource;

    @Parameter(names = "--replicas", description = "Resource replicas.", order = 4)
    private Integer replicas;

    @Parameter(names = "--host", description = "Coordinator host.", order = 5)
    private String host;

    @Parameter(names = "--port", description = "Coordinator port.", order = 6)
    private Integer port;

    @Parameter(names = "--printLog", description = "Whether print log.", order = 7)
    private boolean printLog = false;

    @Parameter(names = "--executor", description = "Executor name(instance id).", order = 8)
    private boolean executor;

    @Parameter(names = "--table", description = "Table name.", order = 9)
    private boolean table;

    @Parameter(names = "--name", order = 10)
    private String name;

    @Parameter(names = "--file", order = 11)
    private String file;

    @Parameter(names = "--batch", order = 12)
    private int batch = 1000;

    @Parameter(names = "--parallel", order = 13)
    private int parallel = 10;

    @Parameter(names = "--showSql", order = 14)
    private boolean showSql;

    @Parameter(names = "--delete", order = 15)
    private boolean delete;

    @Parameter(names = "--tag", description = "Tag.", order = 16)
    private String tag;

    public static void main(String[] args) throws Exception {
        Tools tools = new Tools();
        JCommander commander = new JCommander(tools);
        commander.parse(args);
        tools.exec(commander);
    }

    public void exec(JCommander commander) throws Exception {
        if (help) {
            commander.usage();
            return;
        }
        YAML.parse(new FileInputStream(this.config), Map.class)
            .forEach((k, v) -> DingoConfiguration.instance().set(k.toString(), v));

        switch (cmd.toUpperCase()) {
            case "SQLLINE":
                SqlLineHandler.handler(new String[0]);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + cmd);
        }
    }

}
