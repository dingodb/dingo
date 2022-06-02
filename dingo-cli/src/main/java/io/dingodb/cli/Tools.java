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
import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.exec.Services;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.api.LogLevelApi;
import io.dingodb.server.driver.DriverProxyService;
import lombok.extern.slf4j.Slf4j;
import sqlline.DingoSqlline;
import sqlline.SqlLine;

import java.net.DatagramSocket;
import java.util.ServiceLoader;

@Slf4j
public class Tools {

    @Parameter(names = "--help", help = true, order = 0)
    private boolean help;

    @Parameter(description = "command", order = 1, required = true)
    private String cmd;

    @Parameter(names = "--config", description = "Config file path.", order = 2)
    private String config;

    @Parameter(names = "--port", description = "Coordinator port.", order = 6)
    private Integer port = 8765;

    @Parameter(names = "--class", description = "Log class name.", order = 7)
    private String className;

    @Parameter(names = "--level", description = "Log level.", order = 8)
    private String level;

    @Parameter(names = "--serverHost", description = "Server host.", order = 9)
    private String serverHost;

    @Parameter(names = "--serverPort", description = "Server port.", order = 10)
    private Integer serverPort;

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
        DingoConfiguration.parse(this.config);
        NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

        switch (cmd.toUpperCase()) {
            case "SQLLINE":
                log.info("Listen exchange port {}.", listenRandomPort(netService));
                DingoSqlline sqlline = new DingoSqlline();
                sqlline.connect();
                Services.initControlMsgService();
                SqlLine.Status status = sqlline.begin(new String[0], null, true);
                if (!Boolean.getBoolean("sqlline.system.exit")) {
                    System.exit(status.ordinal());
                }
                break;
            case "DRIVER":
                Services.initControlMsgService();
                netService.listenPort(port);
                DingoConfiguration.instance().setPort(port);
                DriverProxyService driverProxyService = new DriverProxyService();
                driverProxyService.start();
                break;
            case "LEVEL":
                if (className == null || level == null || serverHost == null || serverPort <= 0) {
                    log.error("Class:{} || Level:{} || serverHost:{} || serverPort:{} Parameters cannot be null",
                        className, level, serverHost, serverHost);
                    break;
                }
                LogLevelApi proxy = netService.apiRegistry()
                    .proxy(LogLevelApi.class, () -> new Location(serverHost, serverPort));
                try {
                    proxy.setLevel(className, level);
                } catch (ClassNotFoundException e) {
                    log.error("Class:{} not found, ", className, e);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + cmd);
        }
    }

    private int listenRandomPort(NetService netService)  {
        int times = 3;
        while (times-- > 0) {
            try {
                DatagramSocket datagramSocket = new DatagramSocket();
                netService.listenPort(datagramSocket.getLocalPort());
                DingoConfiguration.instance().getExchange().setPort(datagramSocket.getLocalPort());
                datagramSocket.close();
                return datagramSocket.getLocalPort();
            } catch (Exception e) {
                log.error("Listen port error.", e);
            }
        }
        throw new RuntimeException("Listen port failed.");
    }

}
