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

import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.mysql.util.CharsetUtil;
import io.dingodb.driver.DingoConnection;
import io.dingodb.driver.ServerMeta;
import io.dingodb.driver.mysql.netty.MysqlIdleStateHandler;
import io.dingodb.driver.mysql.netty.MysqlNettyServer;
import io.dingodb.driver.mysql.packet.AuthPacket;
import io.netty.channel.socket.SocketChannel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.util.Map;
import java.util.Set;

import static io.dingodb.calcite.executor.SetOptionExecutor.CONNECTION_CHARSET;
import static io.dingodb.calcite.executor.SetOptionExecutor.RESULTS_CHARSET;

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

    public boolean querySpecial;
    public String querySpecialId;

    public MysqlConnection(SocketChannel channel) {
        this.channel = channel;
    }

    public void setConnection(DingoConnection dingoConnection) {
        connection = dingoConnection;
        this.id = dingoConnection.id;
        setCharsetIndex(authPacket);
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
            Map connectionMap = ExecutionEnvironment.INSTANCE.sessionUtil.connectionMap;
            connectionMap.remove("mysql:" + threadId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        LogUtils.info(log, "mysql connections count:" + MysqlNettyServer.connections.size());
    }

    public boolean setCharsetIndex(AuthPacket authPacket) {
        if (authPacket == null) {
            return false;
        }
        String charset = CharsetUtil.getCharset(authPacket.charsetIndex);
        if (charset != null) {
            charset = CharsetUtil.getJavaCharset(charset);
            try {
                byte[] val = "test".getBytes(charset);
            } catch (UnsupportedEncodingException e) {
                LogUtils.error(log, "set charset index error, charset:{}", charset);
                return false;
            }
            try {
                connection.setClientInfo(CONNECTION_CHARSET, charset);
                connection.setClientInfo(RESULTS_CHARSET, charset);
                return true;
            } catch (SQLClientInfoException e) {
                LogUtils.error(log, "set charset index error,reason:{}", e.getMessage());
            }
        }
        return false;
    }
}
