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

package io.dingodb.driver.mysql.netty;

import io.dingodb.common.log.LogUtils;
import io.dingodb.common.mysql.ExtendedClientCapabilities;
import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.driver.mysql.packet.EOFPacket;
import io.dingodb.driver.mysql.packet.MysqlPacketFactory;
import io.dingodb.driver.mysql.packet.OKPacket;
import io.dingodb.driver.mysql.packet.ResultSetRowPacket;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.calcite.executor.SetOptionExecutor.CONNECTION_CHARSET;
import static io.dingodb.common.util.Utils.getCharacterSet;
import static io.dingodb.driver.mysql.command.MysqlResponseHandler.getArrayObject;

@Slf4j
public class AsyncStreamReader implements Runnable {
    ResultSet resultSet;
    AtomicLong packetId;
    MysqlConnection mysqlConnection;
    int initServerStatus;
    Statement statement;

    public AsyncStreamReader(
        ResultSet resultSet,
        AtomicLong packetId,
        MysqlConnection mysqlConnection,
        int initServerStatus,
        Statement statement
    ) {
        this.resultSet = resultSet;
        this.packetId = packetId;
        this.mysqlConnection = mysqlConnection;
        this.initServerStatus = initServerStatus;
        this.statement = statement;
    }

    @Override
    public void run() {
        int columnCount;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            columnCount = metaData.getColumnCount();
            String typeName;
            AtomicLong cnt = new AtomicLong(0);
            while (resultSet.next()) {
                ResultSetRowPacket resultSetRowPacket = new ResultSetRowPacket();
                long nextId = packetId.getAndIncrement();
                resultSetRowPacket.packetId = (byte) nextId;
                String characterSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
                characterSet = getCharacterSet(characterSet);
                resultSetRowPacket.setCharacterSet(characterSet);
                for (int i = 1; i <= columnCount; i++) {
                    Object val = resultSet.getObject(i);
                    typeName = metaData.getColumnTypeName(i);
                    if (typeName.equalsIgnoreCase("BOOLEAN")) {
                        if (val != null) {
                            if ("TRUE".equalsIgnoreCase(val.toString())) {
                                val = "1";
                            } else {
                                val = "0";
                            }
                        }
                    } else if (typeName.equalsIgnoreCase("ARRAY")) {
                        val = getArrayObject(mysqlConnection, val);
                    } else if (typeName.equalsIgnoreCase("BIT")) {
                        if (val != null) {
                            val = "0x" + Long.toHexString((long) val);
                        }
                    }
                    resultSetRowPacket.addColumnValue(val);
                }
                cnt.incrementAndGet();
                resultSetRowPacket.write(outputStream);
                if (cnt.get() % 100000 == 0) {
                    LogUtils.info(log, "write big data. " +
                            " cnt:{}, packetId:{}",
                        cnt.get(), packetId.get());
                    mysqlConnection.writeAndFlush(outputStream.toByteArray());
                    try {
                        outputStream.close();
                    } catch (IOException e) {
                        LogUtils.error(log, e.getMessage(), e);
                    }
                    outputStream = new ByteArrayOutputStream();
                }
            }
        } catch (SQLException e) {
            LogUtils.error(log, "stream reader failed, reason:{}", e.getMessage(), e);
        } finally {
            boolean deprecateEof = (mysqlConnection.authPacket.extendClientFlags
                & ExtendedClientCapabilities.CLIENT_DEPRECATE_EOF) != 0;
            if (deprecateEof) {
                OKPacket okEofPacket = MysqlPacketFactory.getInstance().getOkEofPacket(
                    0, packetId, initServerStatus
                );
                okEofPacket.write(outputStream);
            } else {
                EOFPacket eofPacket = MysqlPacketFactory.getEofPacket(packetId, initServerStatus);
                eofPacket.write(outputStream);
            }
            mysqlConnection.writeAndFlush(outputStream.toByteArray());
            try {
                outputStream.close();
                mysqlConnection.writeAndFlush(new byte[]{});
                resultSet.close();
                statement.close();
            } catch (SQLException | IOException e) {
                LogUtils.error(log, e.getMessage(), e);
            }
        }
    }
}
