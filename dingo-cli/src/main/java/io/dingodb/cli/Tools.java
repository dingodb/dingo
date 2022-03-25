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
import io.dingodb.common.config.ClusterOptions;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.config.DingoOptions;
import io.dingodb.common.config.ExchangeOptions;
import io.dingodb.exec.Services;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.client.config.ClientOptions;
import io.dingodb.server.driver.DriverProxyService;
import lombok.extern.slf4j.Slf4j;
import sqlline.DingoSqlline;
import sqlline.SqlLine;

import java.net.DatagramSocket;
import java.util.ServiceLoader;

@Slf4j
public class Tools {

    private static final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    @Parameter(names = "--help", help = true, order = 0)
    private boolean help;

    @Parameter(description = "command", order = 1, required = true)
    private String cmd;

    @Parameter(names = "--config", description = "Config file path.", order = 2)
    private String config;

    @Parameter(names = "--port", description = "Coordinator port.", order = 6)
    private Integer port = 8765;

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
        DingoConfiguration.configParse(this.config);
        ClientOptions clientOpts = DingoConfiguration.instance().getAndConvert("client",
            ClientOptions.class, ClientOptions::new);
        ClusterOptions clusterOpts = DingoConfiguration.instance().getAndConvert("cluster",
            ClusterOptions.class, ClusterOptions::new);
        initDingoOptions(clientOpts, clusterOpts);

        log.info("tools configuration: {}.", clientOpts);
        log.info("instance configuration: {}.", DingoOptions.instance());

        switch (cmd.toUpperCase()) {
            case "SQLLINE":
                log.info("Listen exchange port {}.", listenRandomPort());
                DingoSqlline sqlline = new DingoSqlline();
                sqlline.connect();
                SqlLine.Status status = sqlline.begin(new String[0], null, true);
                if (!Boolean.getBoolean("sqlline.system.exit")) {
                    System.exit(status.ordinal());
                }
                break;
            case "DRIVER":
                Services.initNetService();
                netService.listenPort(port);
                DriverProxyService driverProxyService = new DriverProxyService();
                driverProxyService.start();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + cmd);
        }
    }

    private int listenRandomPort()  {
        int times = 3;
        while (times-- > 0) {
            try {
                DatagramSocket datagramSocket = new DatagramSocket();
                netService.listenPort(datagramSocket.getLocalPort());
                DingoOptions.instance().getExchange().setPort(datagramSocket.getLocalPort());
                datagramSocket.close();
                return datagramSocket.getLocalPort();
            } catch (Exception e) {
                log.error("Listen port error.", e);
            }
        }
        throw new RuntimeException("Listen port failed.");
    }

    private void initDingoOptions(final ClientOptions opts, final ClusterOptions clusterOpts) {
        DingoOptions.instance().setClusterOpts(clusterOpts);
        DingoOptions.instance().setIp(opts.getIp());
        ExchangeOptions exchangeOptions = new ExchangeOptions();
        exchangeOptions.setPort(port);
        // port will be set later
        DingoOptions.instance().setExchange(exchangeOptions);
        DingoOptions.instance().setCoordOptions(opts.getOptions().getCoordOptions());
        DingoOptions.instance().setCliOptions(opts.getOptions().getCliOptions());
        int capacity = opts.getOptions().getCapacity();
        if (capacity != 0) {
            DingoOptions.instance().setQueueCapacity(capacity);
        }
    }

}
