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
import io.dingodb.common.config.ClusterOptions;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.config.DingoOptions;
import io.dingodb.common.config.ExchangeOptions;
import io.dingodb.driver.server.ServerMetaFactory;
import io.dingodb.exec.Services;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.client.config.ClientOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;

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
                SqlLineHandler.handler(new String[0]);
                break;
            case "DRIVER":
                DatagramSocket datagramSocket = new DatagramSocket();
                netService.listenPort(datagramSocket.getLocalPort());
                DingoOptions.instance().getExchange().setPort(datagramSocket.getLocalPort());
                datagramSocket.close();

                Services.initNetService();
                HttpServer server = Main.start(
                    new String[]{ServerMetaFactory.class.getCanonicalName()},
                    8765,
                    AvaticaJsonHandler::new
                );
                server.join();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + cmd);
        }
    }

    private void initDingoOptions(final ClientOptions opts, final ClusterOptions clusterOpts) {
        DingoOptions.instance().setClusterOpts(clusterOpts);
        DingoOptions.instance().setIp(opts.getIp());
        ExchangeOptions exchangeOptions = new ExchangeOptions();
        // port will be set later
        DingoOptions.instance().setExchange(exchangeOptions);
        DingoOptions.instance().setCoordOptions(opts.getOptions().getCoordOptions());
        DingoOptions.instance().setCliOptions(opts.getOptions().getCliOptions());
    }

}
