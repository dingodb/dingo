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

import io.dingodb.net.Channel;
import io.dingodb.net.NetAddress;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.SimpleMessage;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ServiceLoader;

import static io.dingodb.server.SqlExecutor.SQL_EXECUTOR_TAG;
import static io.dingodb.server.SqlExecutor.SQL_SEP;

@Slf4j
public class SqlHandler {

    public static final String SPACE = " ";
    private static final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    public static void handler(String host, int port, String sql) throws Exception {
        Channel channel = netService.newChannel(NetAddress.builder().host(host).port(port).build());
        String type = sql.split(SPACE)[0];
        channel.send(SimpleMessage.builder()
            .tag(SQL_EXECUTOR_TAG)
            .content(String.format("%s%s%s", type, SQL_SEP, sql).getBytes(StandardCharsets.UTF_8))
            .build());
        channel.registerMessageListener((msg, ch) -> {
            System.out.printf("%s\n", new String(msg.toBytes()));
        });
        while (channel.status() != Channel.Status.CLOSE) {
            Thread.sleep(1000);
        }
    }

}
