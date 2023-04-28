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
import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.driver.mysql.packet.ColumnPacket;
import io.dingodb.driver.mysql.packet.ColumnsNumberPacket;
import io.dingodb.driver.mysql.packet.ERRPacket;
import io.dingodb.driver.mysql.packet.MysqlPacketFactory;
import io.dingodb.driver.mysql.packet.OKPacket;
import io.dingodb.driver.mysql.packet.PreparePacket;
import io.dingodb.driver.mysql.packet.ResultSetRowPacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MysqlResponseHandler {

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
            OKPacket okPacket = factory.getOkEofPacket(0, packetId, 0);
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
        boolean deprecateEof = false;
        if ((mysqlConnection.authPacket.extendClientFlags
            & ExtendedClientCapabilities.CLIENT_DEPRECATE_EOF) != 0) {
            deprecateEof = true;
        }
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

            if (deprecateEof) {
                handlerRowPacket(resultSet, packetId, mysqlConnection, buffer, columnCount);
                OKPacket okEofPacket = factory.getOkEofPacket(0, packetId, 0);
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
        while (resultSet.next()) {
            ResultSetRowPacket resultSetRowPacket = new ResultSetRowPacket();
            resultSetRowPacket.packetId = (byte) packetId.getAndIncrement();
            for (int i = 1; i <= columnCount; i ++) {
                resultSetRowPacket.addColumnValue(resultSet.getString(i));
            }
            resultSetRowPacket.write(buffer);
            int writerIndex = buffer.writerIndex();
            if (writerIndex > 1048576) {
                mysqlConnection.channel.writeAndFlush(buffer);
                buffer.clear();
            }
        }
    }

    public static void responseError(AtomicLong packetId,
                                     SocketChannel channel,
                                     io.dingodb.common.mysql.constant.ErrorCode errorCode) {
        ERRPacket ep = new ERRPacket();
        ep.packetId = (byte) packetId.getAndIncrement();
        ep.capabilities = MysqlServer.getServerCapabilities();
        ep.errorCode = errorCode.code;
        ep.sqlState = errorCode.sqlState;
        ep.errorMessage =
            errorCode.message;
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        ep.write(buffer);
        channel.writeAndFlush(buffer);
    }

    public static void responseError(AtomicLong packetId,
                                     SocketChannel channel,
                                     SQLException exception) {
        ERRPacket ep = new ERRPacket();
        ep.packetId = (byte) packetId.getAndIncrement();
        ep.capabilities = MysqlServer.getServerCapabilities();
        ep.errorCode = exception.getErrorCode();
        ep.sqlState = exception.getSQLState();
        ep.errorMessage = exception.getMessage();
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        ep.write(buffer);
        channel.writeAndFlush(buffer);
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
}
