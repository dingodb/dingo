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
import io.dingodb.driver.mysql.netty.MysqlNettyServer;
import lombok.extern.slf4j.Slf4j;

import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.TimeUnit;

@Slf4j
public class IdleTimeWatcher implements Observer {

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
        }

    }
}
