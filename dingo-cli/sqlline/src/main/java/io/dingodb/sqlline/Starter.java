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

package io.dingodb.sqlline;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.exec.Services;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import lombok.extern.slf4j.Slf4j;
import sqlline.DingoSqlline;
import sqlline.SqlLine;

import java.net.DatagramSocket;
import java.util.ServiceLoader;

@Slf4j
public class Starter {
    @Parameter(names = "--help", help = true, order = 0)
    private boolean help;

    @Parameter(names = "--config", description = "Config file path.", order = 2)
    private String config;

    @Parameter(names = "--local", description = "Local sqlline.", order = 3)
    private boolean local = false;

    @Parameter(names = "--port", description = "Executor port.", order = 6)
    private Integer port = 8765;

    @Parameter(names = "--host", description = "Executor host.", order = 7)
    private String host = "localhost";

    @Parameter(names = "--user", description = "User name.", order = 8)
    private String user = "root";

    @Parameter(names = "--password", description = "Password.", order = 9)
    private String password = "";

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
        DingoSqlline sqlline = new DingoSqlline();


        if (local) {
            // todo keep this sqlline mode?
            //DingoConfiguration.parse(this.config);
            //NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
            //
            //log.info("Listen exchange port {}.", listenRandomPort(netService));
            //sqlline.connect();
            //Services.initControlMsgService();
            //SqlLine.Status status = sqlline.begin(new String[0], null, true);
            //if (!Boolean.getBoolean("sqlline.system.exit")) {
            //    System.exit(status.ordinal());
            //}
            throw new UnsupportedOperationException("Unsupported local mode.");
        } else {
            sqlline.connect(host, port, user, password);
            SqlLine.Status status = sqlline.begin(new String[0], null, true);
            if (!Boolean.getBoolean("sqlline.system.exit")) {
                System.exit(status.ordinal());
            }
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
