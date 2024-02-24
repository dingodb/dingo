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

package io.dingodb.driver.mysql;

import io.dingodb.driver.DingoConnection;
import io.dingodb.driver.ServerMeta;
import io.dingodb.driver.mysql.netty.MysqlIdleStateHandler;
import io.dingodb.driver.mysql.netty.MysqlNettyServer;
import io.dingodb.driver.mysql.packet.AuthPacket;
import io.netty.channel.socket.SocketChannel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;

@Slf4j
public class MysqlConnection {
    @Getter
    @Setter
    private String id;

    @Getter
    @Setter
    private int threadId;

    public SocketChannel channel;

    @Getter
    private Connection connection;

    public boolean authed;

    public AuthPacket authPacket;

    public MysqlIdleStateHandler mysqlIdleStateHandler;

    public volatile Boolean passwordExpire = false;

    public MysqlConnection(SocketChannel channel) {
        this.channel = channel;
    }

    public void setConnection(DingoConnection dingoConnection) {
        connection = dingoConnection;
        this.id = dingoConnection.id;
    }

    public void close() {
        if (channel.isActive()) {
            channel.disconnect();
        }
        try {
            if (connection != null) {
                DingoConnection dingoConnection = (DingoConnection) connection;
                Set<Integer> statementIds = dingoConnection.statementMap.keySet();
                for (Integer statementId : statementIds) {
                    dingoConnection.statementMap.get(statementId).close();
                }
                if (!connection.isClosed()) {
                    connection.close();
                }
            }
            Map connectionMap = ServerMeta.getInstance().connectionMap;
            connectionMap.remove("mysql:" + threadId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("mysql connections count:" + MysqlNettyServer.connections.size());
    }
}
