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

import io.dingodb.common.mysql.client.SessionVariableChange;
import io.dingodb.driver.DingoConnection;
import io.dingodb.driver.ServerMeta;
import io.dingodb.driver.mysql.netty.MysqlNettyServer;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SessionVariableChangeWatcher implements Observer {

    @Override
    public void update(Observable observable, Object arg) {
        SessionVariableChange sessionVariable = (SessionVariableChange) arg;
        if ("wait_timeout".equalsIgnoreCase(sessionVariable.getName())) {
            MysqlConnection connection = MysqlNettyServer.connections.get(sessionVariable.getId());
            if (connection != null && !connection.authPacket.interActive) {
                connection.mysqlIdleStateHandler.setIdleTimeout(Long.parseLong(sessionVariable.getValue()),
                    TimeUnit.SECONDS);
                log.info("update connection idle time:" + sessionVariable);
            }
        } else if ("interactive_timeout".equalsIgnoreCase(sessionVariable.getName())) {
            MysqlConnection connection = MysqlNettyServer.connections.get(sessionVariable.getId());
            if (connection != null && connection.authPacket.interActive) {
                connection.mysqlIdleStateHandler.setIdleTimeout(Long.parseLong(sessionVariable.getValue()),
                    TimeUnit.SECONDS);
                log.info("update interactive connection idle time:" + sessionVariable);
            }
        } else if ("@password_reset".equalsIgnoreCase(sessionVariable.getName())) {
            MysqlConnection connection = MysqlNettyServer.connections.get(sessionVariable.getId());
            connection.passwordExpire = false;
        } else if ("@connection_kill".equalsIgnoreCase(sessionVariable.getName())) {
            boolean isNumeric = sessionVariable.getValue().matches("\\d+");
            if (isNumeric) {
                Optional<String> matchMysqlConn = MysqlNettyServer.connections
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().getThreadId() == Integer.parseInt(sessionVariable.getValue()))
                    .map(Map.Entry::getKey)
                    .findFirst();
                if (matchMysqlConn.isPresent()) {
                    MysqlConnection connection = MysqlNettyServer.connections.get(matchMysqlConn.get());
                    if (connection != null) {
                        connection.close();
                    }
                }
            } else {
                if (ServerMeta.getInstance().connectionMap.containsKey(sessionVariable.getValue())) {
                    DingoConnection dingoConnection = ServerMeta.getInstance()
                        .connectionMap.get(sessionVariable.getValue());
                    try {
                        dingoConnection.close();
                        ServerMeta.getInstance().connectionMap.remove(sessionVariable.getValue());
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

    }
}
