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
import io.dingodb.common.mysql.constant.ColumnStatus;
import io.dingodb.common.mysql.constant.ColumnType;
import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.driver.mysql.packet.ColumnPacket;
import io.dingodb.driver.mysql.packet.ColumnsNumberPacket;
import io.dingodb.driver.mysql.packet.EOFPacket;
import io.dingodb.driver.mysql.packet.ERRPacket;
import io.dingodb.driver.mysql.packet.OKPacket;
import io.dingodb.driver.mysql.packet.ResultSetPacket;
import io.dingodb.driver.mysql.packet.ResultSetRowPacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.common.mysql.constant.ServerStatus.SERVER_STATUS_AUTOCOMMIT;

@Slf4j
public class MysqlResponseHandler {

    public static void responseShowField(ResultSet resultSet,
                                         AtomicLong packetId,
                                         MysqlConnection mysqlConnection) {
        // 1. column packet
        // 2. ok packet
        try {
            List<ColumnPacket> columnPackets = getColumnPackets(packetId, resultSet, true);
            ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
            for (ColumnPacket columnPacket : columnPackets) {
                columnPacket.write(buffer);
            }
            OKPacket okPacket = getOkPacket(0, packetId, 0);
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
        // 4. ok packet

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
            ResultSetPacket resultSetPacket = new ResultSetPacket();
            resultSetPacket.extendClientCapabilities = mysqlConnection.authPacket.extendClientFlags;
            resultSetPacket.capabilities = mysqlConnection.authPacket.clientFlags;
            ResultSetMetaData metaData = resultSet.getMetaData();
            ColumnsNumberPacket columnsNumberPacket = new ColumnsNumberPacket();
            columnsNumberPacket.packetId = (byte) packetId.getAndIncrement();
            int columnCount = metaData.getColumnCount();
            columnsNumberPacket.columnsNumber = columnCount;
            resultSetPacket.columnsNumber = columnsNumberPacket;

            List<ColumnPacket> columns = getColumnPackets(packetId, resultSet, false);

            resultSetPacket.columns = columns;

            if (deprecateEof) {
                while (resultSet.next()) {
                    ResultSetRowPacket resultSetRowPacket = new ResultSetRowPacket();
                    resultSetRowPacket.packetId = (byte) packetId.getAndIncrement();
                    for (int i = 1; i <= columnCount; i ++) {
                        resultSetRowPacket.values.add(resultSet.getString(i));
                    }
                    resultSetPacket.rows.add(resultSetRowPacket);
                }
                // response ok
                OKPacket okPacket = getOkPacket(0, packetId, 0);
                resultSetPacket.okPacket = okPacket;
            } else {
                // intermediate eof
                EOFPacket columnsEof = new EOFPacket();
                columnsEof.packetId = (byte) packetId.getAndIncrement();
                columnsEof.header = (byte) 0xfe;
                columnsEof.warningCount = 0;
                columnsEof.statusFlags = SERVER_STATUS_AUTOCOMMIT;
                resultSetPacket.columnsEof = columnsEof;
                // row packet...
                while (resultSet.next()) {
                    ResultSetRowPacket resultSetRowPacket = new ResultSetRowPacket();
                    for (int i = 1; i <= columnCount; i ++) {
                        resultSetRowPacket.values.add(resultSet.getString(i));
                    }
                }
                // response EOF
                EOFPacket responseEof = new EOFPacket();
                responseEof.packetId = (byte) packetId.getAndIncrement();
                responseEof.header = (byte) 0xfe;
                responseEof.warningCount = 0;
                responseEof.statusFlags = SERVER_STATUS_AUTOCOMMIT;
                resultSetPacket.rowsEof = responseEof;
            }

            ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
            resultSetPacket.write(buffer);
            mysqlConnection.channel.writeAndFlush(buffer);
        } catch (SQLException e) {
            responseError(packetId, mysqlConnection.channel, e);
        }
    }

    @NonNull
    public static OKPacket getOkPacket(int affected, AtomicLong packetId, int serverStatus) {
        OKPacket okPacket = new OKPacket();
        okPacket.capabilities = MysqlServer.getServerCapabilities();
        okPacket.affectedRows = affected;
        okPacket.packetId = (byte) packetId.getAndIncrement();
        int status = SERVER_STATUS_AUTOCOMMIT;
        if (serverStatus != 0) {
            status |= serverStatus;
        }
        okPacket.serverStatus = status;
        okPacket.insertId = 0;
        okPacket.header = (byte) 0xfe;
        return okPacket;
    }

    @NonNull
    private static List<ColumnPacket> getColumnPackets(AtomicLong packetId,
                                                       ResultSet resultSet,
                                                       boolean showFields) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        List<ColumnPacket> columns = new ArrayList<>();
        int columnCount = metaData.getColumnCount();
        String catalog = "def";
        if (showFields) {
            while (resultSet.next()) {
                ColumnPacket columnPacket = new ColumnPacket();
                columnPacket.name = resultSet.getString("COLUMN_NAME");
                columnPacket.orgName = columnPacket.name;
                columnPacket.packetId = (byte) packetId.getAndIncrement();
                columnPacket.catalog = catalog;
                columnPacket.schema = resultSet.getString("TABLE_SCHEM");
                columnPacket.table = resultSet.getString("TABLE_NAME");
                columnPacket.orgTable = resultSet.getString("TABLE_NAME");
                columnPacket.characterSet = 45;
                columnPacket.columnLength = resultSet.getInt("COLUMN_SIZE");
                String dataType = resultSet.getString("DATA_TYPE");
                columnPacket.type = (byte) (ColumnType.typeMapping.get(dataType) & 0xff);
                columnPacket.flags = (short) getColumnFlags(resultSet);
                columnPacket.decimals = 0x00;
                columns.add(columnPacket);
            }
        } else {
            String table = metaData.getTableName(1);
            String schema = metaData.getSchemaName(1);
            table = table != null ? table : "";
            schema = schema != null ? schema : "";
            for (int i = 1; i <= columnCount; i++) {
                ColumnPacket columnPacket = new ColumnPacket();
                columnPacket.packetId = (byte) packetId.getAndIncrement();
                columnPacket.catalog = catalog;
                columnPacket.schema = schema;
                columnPacket.table = table;
                columnPacket.orgTable = table;
                columnPacket.name = metaData.getColumnLabel(i);
                columnPacket.orgName = metaData.getColumnName(i);
                columnPacket.characterSet = 45;
                columnPacket.columnLength = metaData.getColumnDisplaySize(i);
                columnPacket.type = (byte) (getColumnType(metaData, i) & 0xff);
                columnPacket.flags = (short) getColumnFlags(metaData, i);
                columnPacket.decimals = 0x00;
                columns.add(columnPacket);
            }
        }
        return columns;
    }

    public static int getColumnType(ResultSetMetaData metaData, int column) {
        try {
            return ColumnType.typeMapping.get(metaData.getColumnTypeName(column));
        } catch (SQLException e) {
            return ColumnType.FIELD_TYPE_VAR_STRING;
        }
    }

    public static int getColumnFlags(ResultSetMetaData metaData, int column) {
        try {
            int columnFlags = 0;
            // 0 not null  1 nullable
            int isNullable =  metaData.isNullable(column);
            columnFlags |= isNullable;

            String columnTypeName = metaData.getColumnTypeName(column);
            if (columnTypeName.equals("BLOB")) {
                columnFlags |= ColumnStatus.COLUMN_BLOB;
            } else if (columnTypeName.equals("TIMESTAMP")) {
                columnFlags |= ColumnStatus.COLUMN_TIMESTAMP;
            } else if (columnTypeName.equals("ARRAY") || columnTypeName.equals("MULTISET")) {
                columnFlags |= ColumnStatus.COLUMN_SET;
            }

            return columnFlags;
        } catch (Exception e) {
            return 0;
        }
    }

    public static int getColumnFlags(ResultSet resultSet) {
        try {
            int columnFlags = 0;
            // 0 not null  1 nullable
            int isNullable =  resultSet.getInt("NULLABLE");
            columnFlags |= isNullable;

            String columnTypeName = resultSet.getString("TYPE_NAME");
            if (columnTypeName.equals("BLOB")) {
                columnFlags |= ColumnStatus.COLUMN_BLOB;
            } else if (columnTypeName.equals("TIMESTAMP")) {
                columnFlags |= ColumnStatus.COLUMN_TIMESTAMP;
            } else if (columnTypeName.equals("ARRAY") || columnTypeName.equals("MULTISET")) {
                columnFlags |= ColumnStatus.COLUMN_SET;
            }

            return columnFlags;
        } catch (Exception e) {
            return 0;
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
                                     SQLException e) {
        ERRPacket ep = new ERRPacket();
        ep.packetId = (byte) packetId.getAndIncrement();
        ep.capabilities = MysqlServer.getServerCapabilities();
        ep.errorCode = e.getErrorCode();
        ep.sqlState = e.getSQLState();
        ep.errorMessage = e.getMessage();
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        ep.write(buffer);
        channel.writeAndFlush(buffer);
    }

    public static void responseOk(OKPacket okPacket, SocketChannel channel) {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        okPacket.write(buffer);
        channel.writeAndFlush(buffer);
    }
}
