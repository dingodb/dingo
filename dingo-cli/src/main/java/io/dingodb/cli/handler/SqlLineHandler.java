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

package io.dingodb.cli.handler;

import io.dingodb.common.config.DingoOptions;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import sqlline.DingoSqlline;
import sqlline.SqlLine;

import java.net.DatagramSocket;
import java.util.ServiceLoader;

public class SqlLineHandler {

    private static final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    public static void handler(String[] args) throws Exception {
        DatagramSocket datagramSocket = new DatagramSocket();
        netService.listenPort(datagramSocket.getLocalPort());
        DingoOptions.instance().getExchange().setPort(datagramSocket.getLocalPort());
        datagramSocket.close();
        DingoSqlline sqlline = new DingoSqlline();
        sqlline.connect();
        SqlLine.Status status = sqlline.begin(args, null, true);
        if (!Boolean.getBoolean("sqlline.system.exit")) {
            System.exit(status.ordinal());
        }
    }

}
