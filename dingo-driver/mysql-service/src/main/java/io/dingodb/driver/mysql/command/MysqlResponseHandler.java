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

import io.dingodb.common.mysql.ExtendedClientCapabilities;
import io.dingodb.common.mysql.MysqlServer;
import io.dingodb.common.mysql.constant.ServerStatus;
import io.dingodb.driver.DingoConnection;
import io.dingodb.driver.common.DingoArray;
import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.driver.mysql.packet.ColumnPacket;
import io.dingodb.driver.mysql.packet.ColumnsNumberPacket;
import io.dingodb.driver.mysql.packet.ERRPacket;
import io.dingodb.driver.mysql.packet.MysqlPacketFactory;
import io.dingodb.driver.mysql.packet.OKPacket;
import io.dingodb.driver.mysql.packet.PreparePacket;
import io.dingodb.driver.mysql.packet.PrepareResultSetRowPacket;
import io.dingodb.driver.mysql.packet.ResultSetRowPacket;
import io.dingodb.exec.exception.TaskFinException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.calcite.operation.SetOptionOperation.CONNECTION_CHARSET;
import static io.dingodb.common.util.Utils.getCharacterSet;
import static io.dingodb.common.util.Utils.getDateByTimezone;
import static io.dingodb.driver.mysql.command.MysqlCommands.getInitServerStatus;

@Slf4j
public final class MysqlResponseHandler {

    static MysqlPacketFactory factory = MysqlPacketFactory.getInstance();

    private MysqlResponseHandler() {
    }

    public static void responseShowField(ResultSet resultSet,
                                         AtomicLong packetId,
                                         MysqlConnection mysqlConnection) {
        // 1. column packet
        // 2. ok packet
        try {
            List<ColumnPacket> columnPackets = factory.getColumnPackets(packetId, resultSet, true);
            ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
            for (ColumnPacket columnPacket : columnPackets) {
                columnPacket.write(buffer);
            }
            OKPacket okPacket = factory.getOkEofPacket(0, packetId, ServerStatus.SERVER_STATUS_AUTOCOMMIT);
            okPacket.write(buffer);
            mysqlConnection.channel.writeAndFlush(buffer);
        } catch (SQLException e) {
            responseError(packetId, mysqlConnection.channel, e);
        }
    }

    public static void responseResultSet(ResultSet resultSet,
                                         AtomicLong packetId,
                                   MysqlConnection mysqlConnection) {
        // packet combine:
        // 1. columns count packet
        // 2. column packet
        // 3. rows packet
        // 4. ok eof packet

        // 1. columns count packet
        // 2. column packet
        // 3. eof packet
        // 4. rows packet
        // 5. eof packet
        boolean deprecateEof = (mysqlConnection.authPacket.extendClientFlags
            & ExtendedClientCapabilities.CLIENT_DEPRECATE_EOF) != 0;
        try {
            ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
            ResultSetMetaData metaData = resultSet.getMetaData();
            ColumnsNumberPacket columnsNumberPacket = new ColumnsNumberPacket();
            columnsNumberPacket.packetId = (byte) packetId.getAndIncrement();
            int columnCount = metaData.getColumnCount();
            columnsNumberPacket.columnsNumber = columnCount;
            columnsNumberPacket.write(buffer);

            List<ColumnPacket> columns = factory.getColumnPackets(packetId, resultSet, false);
            for (ColumnPacket columnPacket : columns) {
                columnPacket.write(buffer);
            }

            int initServerStatus = getInitServerStatus((DingoConnection) mysqlConnection.getConnection());
            if (deprecateEof) {
                handlerRowPacket(resultSet, packetId, mysqlConnection, buffer, columnCount);
                OKPacket okEofPacket = factory.getOkEofPacket(
                    0, packetId, initServerStatus
                );
                okEofPacket.write(buffer);
            } else {
                // intermediate eof
                factory.getEofPacket(packetId).write(buffer);
                // row packet...
                handlerRowPacket(resultSet, packetId, mysqlConnection, buffer, columnCount);
                // response EOF
                //resultSetPacket.rowsEof = getEofPacket(packetId);
                factory.getEofPacket(packetId).write(buffer);
            }

            mysqlConnection.channel.writeAndFlush(buffer);
        } catch (SQLException e) {
            responseError(packetId, mysqlConnection.channel, e);
        }
    }

    private static void handlerRowPacket(ResultSet resultSet, AtomicLong packetId, MysqlConnection mysqlConnection,
                                  ByteBuf buffer, int columnCount) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        String typeName;
        while (resultSet.next()) {
            ResultSetRowPacket resultSetRowPacket = new ResultSetRowPacket();
            resultSetRowPacket.packetId = (byte) packetId.getAndIncrement();
            String characterSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
            characterSet = getCharacterSet(characterSet);
            resultSetRowPacket.setCharacterSet(characterSet);
            for (int i = 1; i <= columnCount; i ++) {
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
                }
                resultSetRowPacket.addColumnValue(val);
            }
            resultSetRowPacket.write(buffer);
        }
    }

    public static Object getArrayObject(MysqlConnection mysqlConnection, Object val) throws SQLException {
        List<Object> arrayVal = null;
        if (val instanceof ArrayImpl) {
            ArrayImpl array = (ArrayImpl) val;
            Object o = array.getArray();
            arrayVal = new ArrayList<>();
            int length = Array.getLength(o);
            for (int index = 0; index < length; index ++) {
                arrayVal.add(Array.get(o, index));
            }
            DingoConnection dingoConnection = (DingoConnection) mysqlConnection.getConnection();
            arrayVal = getDateByTimezone(arrayVal, dingoConnection.getTimeZone());
        } else if (val instanceof DingoArray) {
            DingoArray dingoArray = (DingoArray) val;
            arrayVal = (List<Object>) dingoArray.getArray();
        }
        if (arrayVal == null) {
            return "null";
        }
        return StringUtils.join(arrayVal);
    }

    private static void handlerPrepareRowPacket(ResultSet resultSet,
                                                AtomicLong packetId,
                                                MysqlConnection mysqlConnection,
                                                ByteBuf buffer,
                                                int columnCount) throws SQLException {
        while (resultSet.next()) {
            PrepareResultSetRowPacket resultSetRowPacket = new PrepareResultSetRowPacket();
            String characterSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
            characterSet = getCharacterSet(characterSet);
            resultSetRowPacket.setCharacterSet(characterSet);
            resultSetRowPacket.packetId = (byte) packetId.getAndIncrement();
            resultSetRowPacket.setMetaData(resultSet.getMetaData());
            for (int i = 1; i <= columnCount; i ++) {
                resultSetRowPacket.addColumnValue(resultSet.getObject(i), mysqlConnection);
            }
            resultSetRowPacket.write(buffer);
        }
    }

    public static void responseError(AtomicLong packetId,
                                     SocketChannel channel,
                                     io.dingodb.common.mysql.constant.ErrorCode errorCode) {
        responseError(packetId, channel, errorCode, errorCode.message);
    }

    public static void responseError(AtomicLong packetId,
                                     SocketChannel channel,
                                     io.dingodb.common.mysql.constant.ErrorCode errorCode, String message) {
        ERRPacket ep = new ERRPacket();
        ep.packetId = (byte) packetId.getAndIncrement();
        ep.capabilities = MysqlServer.getServerCapabilities();
        ep.errorCode = errorCode.code;
        ep.sqlState = errorCode.sqlState;
        ep.errorMessage = message;
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        ep.write(buffer);
        channel.writeAndFlush(buffer);
    }

    public static void responseError(AtomicLong packetId,
                                     SocketChannel channel,
                                     SQLException exception) {
        exception = errorDingo2Mysql(exception);
        ERRPacket ep = new ERRPacket();
        ep.packetId = (byte) packetId.getAndIncrement();
        ep.capabilities = MysqlServer.getServerCapabilities();
        ep.errorCode = exception.getErrorCode();
        ep.sqlState = exception.getSQLState();
        if (exception.getMessage() == null) {
            ep.errorMessage = "";
        } else if (exception.getMessage().startsWith("Encountered")) {
            ep.errorMessage = "You have an error in your SQL syntax";
        } else {
            ep.errorMessage = exception.getMessage();
        }
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        ep.write(buffer);
        channel.writeAndFlush(buffer);
    }

    public static SQLException errorDingo2Mysql(SQLException e) {
        if (e.getMessage() != null && e.getMessage().contains("Duplicate")) {
            return new SQLException("Duplicate data for key 'PRIMARY'", "23000", 1062);
        } else if (e.getErrorCode() == 9001 && e.getSQLState().equals("45000")) {
            return new SQLException(e.getMessage(), "HY000", 1105);
        } else {
            return e;
        }
    }

    public static void responseOk(OKPacket okPacket, SocketChannel channel) {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        okPacket.write(buffer);
        channel.writeAndFlush(buffer);
    }

    public static void responsePrepare(PreparePacket preparePacket, SocketChannel channel) {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        preparePacket.write(buffer);
        channel.writeAndFlush(buffer);
    }

    public static void responsePrepareExecute(ResultSet resultSet,
                                              AtomicLong packetId,
                                              MysqlConnection mysqlConnection) {
        // packet combine:
        // 1. columns count packet
        // 2. column packet
        // 3. rows packet
        // 4. ok eof packet

        // 1. columns count packet
        // 2. column packet
        // 3. eof packet
        // 4. rows packet
        // 5. eof packet
        boolean deprecateEof = (mysqlConnection.authPacket.extendClientFlags
            & ExtendedClientCapabilities.CLIENT_DEPRECATE_EOF) != 0;
        try {
            ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
            ResultSetMetaData metaData = resultSet.getMetaData();
            ColumnsNumberPacket columnsNumberPacket = new ColumnsNumberPacket();
            columnsNumberPacket.packetId = (byte) packetId.getAndIncrement();
            int columnCount = metaData.getColumnCount();
            columnsNumberPacket.columnsNumber = columnCount;
            columnsNumberPacket.write(buffer);

            List<ColumnPacket> columns = factory.getColumnPackets(packetId, resultSet, false);
            for (ColumnPacket columnPacket : columns) {
                columnPacket.write(buffer);
            }
            int serverStatus = getInitServerStatus((DingoConnection) mysqlConnection.getConnection());

            if (deprecateEof) {
                handlerPrepareRowPacket(resultSet, packetId, mysqlConnection, buffer, columnCount);
                OKPacket okEofPacket = factory.getOkEofPacket(
                    0, packetId, serverStatus
                );
                okEofPacket.write(buffer);
            } else {
                // intermediate eof
                factory.getEofPacket(packetId).write(buffer);
                // row packet...
                handlerPrepareRowPacket(resultSet, packetId, mysqlConnection, buffer, columnCount);
                // response EOF
                //resultSetPacket.rowsEof = getEofPacket(packetId);
                factory.getEofPacket(packetId).write(buffer);
            }

            mysqlConnection.channel.writeAndFlush(buffer);
        } catch (SQLException e) {
            responseError(packetId, mysqlConnection.channel, e);
        }
    }
}
