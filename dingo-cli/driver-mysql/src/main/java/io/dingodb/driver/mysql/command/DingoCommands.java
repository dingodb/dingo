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

package io.dingodb.driver.mysql.command;

import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.driver.mysql.packet.OKPacket;
import io.dingodb.driver.mysql.packet.QueryPacket;
import io.dingodb.driver.mysql.process.MessageProcess;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.common.mysql.constant.ErrorCode.ER_NOT_ALLOWED_COMMAND;

@Slf4j
public class DingoCommands {

    public void executeShowFields(String table, AtomicLong packetId, MysqlConnection mysqlConnection) {
        try {
            ResultSet rs = mysqlConnection.getConnection().getMetaData().getColumns(null, null,
                table, null);
            MysqlResponseHandler.responseShowField(rs, packetId, mysqlConnection);
        } catch (SQLException e) {
            MysqlResponseHandler.responseError(packetId, mysqlConnection.channel, e);
        }
    }

    public void execute(QueryPacket queryPacket,
                        MysqlConnection mysqlConnection) {
        String sqlCommand = new String(queryPacket.message);
        String[] sqlItems = sqlCommand.split(";");
        AtomicLong packetId = new AtomicLong(queryPacket.packetId + 1);
        for (String sql : sqlItems) {
            sql = sql.toLowerCase();
            if (log.isDebugEnabled()) {
                log.debug("receive sql:" + sql);
            }
            executeSingleQuery(sql, packetId, mysqlConnection);
        }
    }

    public void executeSingleQuery(String sql, AtomicLong packetId,
                                   MysqlConnection mysqlConnection) {
        Statement statement = null;
        boolean hasResults;
        try {
            statement = mysqlConnection.getConnection().createStatement();
            hasResults = statement.execute(sql);
            if (hasResults) {
                // select
                do {
                    try (ResultSet rs = statement.getResultSet()) {
                        MysqlResponseHandler.responseResultSet(rs, packetId, mysqlConnection);
                    }
                }
                while (getMoreResults(statement));
            } else {
                // update insert delete
                int count = statement.getUpdateCount();
                OKPacket okPacket = MysqlResponseHandler.getOkPacket(count, packetId, 0);
                MysqlResponseHandler.responseOk(okPacket, mysqlConnection.channel);
            }
        } catch (SQLException sqlException) {
            log.error("sql exception sqlstate:" + sqlException.getSQLState() + ", code:" + sqlException.getErrorCode()
                + ", message:" + sqlException.getMessage());
            MysqlResponseHandler.responseError(packetId, mysqlConnection.channel, sqlException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            // error packet
            MysqlResponseHandler.responseError(packetId, mysqlConnection.channel, ER_NOT_ALLOWED_COMMAND);
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
                MysqlResponseHandler.responseError(packetId, mysqlConnection.channel, ER_NOT_ALLOWED_COMMAND);
            }
        }
    }

    private boolean getMoreResults(Statement stmnt) {
        try {
            return stmnt.getMoreResults();
        } catch (Throwable t) {
            return false;
        }
    }

}
