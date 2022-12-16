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

package io.dingodb.driver.server;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.domain.Domain;
import io.dingodb.common.privilege.PrivilegeDict;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.exec.Services;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.client.flush.FlushHandler;
import io.dingodb.server.driver.DriverProxyServer;
import io.dingodb.server.protocol.Tags;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.ServiceLoader;

@Slf4j
public class Starter {

    @Parameter(names = "--help", help = true, order = 0)
    private boolean help;

    @Parameter(names = "--port", description = "Coordinator port.", order = 6)
    private Integer port = 8765;

    @Parameter(names = "--config", description = "Config file path.", order = 2)
    private String config;

    public static void main(String[] args) throws Exception {
        Starter starter = new Starter();
        JCommander commander = new JCommander(starter);
        commander.parse(args);
        starter.exec(commander);
    }

    public void exec(JCommander commander) throws Exception {
        if (help) {
            commander.usage();
            return;
        }
        Domain.role = DingoRole.JDBC;
        DingoConfiguration.parse(this.config);
        NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

        Services.initControlMsgService();
        netService.listenPort(port);
        DingoConfiguration.instance().setPort(port);
        DriverProxyServer driverProxyServer = new DriverProxyServer();
        FlushHandler.flushHandler.registryFlushChannel();
        log.info("Starting driver server success.");
    }
}
