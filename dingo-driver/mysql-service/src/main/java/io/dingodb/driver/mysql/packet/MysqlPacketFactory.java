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

package io.dingodb.driver.mysql.packet;

import io.dingodb.common.mysql.MysqlServer;
import io.dingodb.common.mysql.constant.ColumnStatus;
import io.dingodb.common.mysql.constant.ColumnType;
import io.dingodb.driver.mysql.NativeConstants;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.common.mysql.constant.ServerStatus.SERVER_STATUS_AUTOCOMMIT;

public class MysqlPacketFactory {
    private static MysqlPacketFactory instance = null;

    public static MysqlPacketFactory getInstance() {
        if (instance == null) {
            synchronized (MysqlPacketFactory.class) {
                if (instance == null) {
                    instance = new MysqlPacketFactory();
                }
            }
        }
        return instance;
    }

    /**
     * for ResultSet OkEof packet.
     * @param affected 0
     * @param packetId increment
     * @param serverStatus serverStatus
     * @return  ok eof packet
     */
    @NonNull
    public OKPacket getOkEofPacket(int affected, AtomicLong packetId, int serverStatus) {
        OKPacket okPacket = newOkPacket(affected, packetId, serverStatus, BigInteger.ZERO, 0);
        okPacket.header = (byte) NativeConstants.TYPE_ID_EOF;
        return okPacket;
    }

    public OKPacket getOkPacket(int affected, AtomicLong packetId, SQLWarning sqlWarning) {
        return getOkPacket(affected, packetId, SERVER_STATUS_AUTOCOMMIT, BigInteger.ZERO, sqlWarning);
    }

    public OKPacket getOkPacket(int affected, AtomicLong packetId, int serverStatus, SQLWarning sqlWarning) {
        return getOkPacket(affected, packetId, serverStatus, BigInteger.ZERO, sqlWarning);
    }

    @NonNull
    public OKPacket getOkPacket(int affected,
                                AtomicLong packetId,
                                int serverStatus,
                                BigInteger lastInsertId,
                                SQLWarning sqlWarning) {
        int warningCount = 0;
        if (sqlWarning != null) {
            warningCount = 1;
        }
        OKPacket okPacket = newOkPacket(affected, packetId, serverStatus, lastInsertId, warningCount);
        okPacket.header = NativeConstants.TYPE_ID_OK;
        return okPacket;
    }

    private OKPacket newOkPacket(int affected,
                                 AtomicLong packetId,
                                 int serverStatus,
                                 BigInteger lastInsertId,
                                 int warningCount) {
        OKPacket okPacket = new OKPacket();
        okPacket.capabilities = MysqlServer.getServerCapabilities();
        okPacket.affectedRows = affected;
        okPacket.packetId = (byte) packetId.getAndIncrement();
//        int status = SERVER_STATUS_AUTOCOMMIT;
//        if (serverStatus != 0) {
//            status |= serverStatus;
//        }
        okPacket.warningCount = warningCount;
        okPacket.serverStatus = serverStatus;
        okPacket.insertId = lastInsertId;
        return okPacket;
    }

    public short getColumnFlags(ResultSetMetaData metaData, int column) {
        try {
            int columnFlags = 0;
            // 0 not null  1 nullable
            int isNullable =  metaData.isNullable(column);
            columnFlags |= isNullable;

            String columnTypeName = metaData.getColumnTypeName(column);
            return (short) combineColumnFlags(columnFlags, columnTypeName);
        } catch (Exception e) {
            return 0;
        }
    }

    public static short getColumnFlags(ResultSet resultSet) {
        try {
            int columnFlags = 0;
            // 0 not null  1 nullable
            int isNullable =  resultSet.getInt("NULLABLE");
            columnFlags |= isNullable;

            String columnTypeName = resultSet.getString("TYPE_NAME");
            return (short) combineColumnFlags(columnFlags, columnTypeName);
        } catch (Exception e) {
            return 0;
        }
    }

    private static int combineColumnFlags(int columnFlags,
                                          String columnTypeName) {
        return combineColumnFlags(columnFlags, columnTypeName, false, false, false);
    }

    /**
     * get column flags.
     * @param columnFlags original column flgs
     * @param columnTypeName name
     * @param isPrimary Dingo was not used, but MySQL was used
     * @param isUnique Dingo was not used, but MySQL was used
     * @param autoIncrement Dingo was not used, but MySQL was used
     * @return int col flg
     */
    private static int combineColumnFlags(int columnFlags,
                                          String columnTypeName,
                                          boolean isPrimary,
                                          boolean isUnique,
                                          boolean autoIncrement) {
        switch (columnTypeName) {
            case "VARBINARY":
                columnFlags |= ColumnStatus.COLUMN_BLOB;
                break;
            case "TIMESTAMP":
                columnFlags |= ColumnStatus.COLUMN_TIMESTAMP;
                break;
            case "ARRAY":
            case "MULTISET":
                columnFlags |= ColumnStatus.COLUMN_SET;
                break;
            default:
                break;
        }
        if (isPrimary) {
            columnFlags |= ColumnStatus.COLUMN_PRIMARY;
        }
        if (isUnique) {
            columnFlags |= ColumnStatus.COLUMN_UNIQUE;
        }
        if (autoIncrement) {
            columnFlags |= ColumnStatus.COLUMN_AUTOINCREMENT;
        }

        return columnFlags;
    }

    public static byte getColumnType(String typeName) {
        try {
            return (byte) (ColumnType.typeMapping.get(typeName) & 0xff);
        } catch (Exception e) {
            return (byte) (ColumnType.FIELD_TYPE_VAR_STRING & 0xff);
        }
    }

    @NonNull
    public List<ColumnPacket> getColumnPackets(AtomicLong packetId,
                                               ResultSet resultSet,
                                               boolean showFields) throws SQLException {

        List<ColumnPacket> columns = new ArrayList<>();
        String catalog = "def";
        if (showFields) {
            while (resultSet.next()) {
                String dataType = resultSet.getString("DATA_TYPE");
                String tableName = resultSet.getString("TABLE_NAME");
                String columnName = resultSet.getString("COLUMN_NAME");
                String schemaName = resultSet.getString("TABLE_SCHEM");
                ColumnPacket columnPacket = getColumnPacket(catalog,
                    schemaName,
                    tableName,
                    tableName,
                    columnName,
                    columnName,
                    MysqlPacket.charsetNumber,
                    resultSet.getInt("COLUMN_SIZE"),
                    getColumnType(dataType),
                    getColumnFlags(resultSet),
                    MysqlPacket.decimals,
                    (byte) packetId.getAndIncrement()
                    );
                columns.add(columnPacket);
            }
        } else {
            addColumnPacketFromMeta(packetId, resultSet.getMetaData(), columns, catalog);
        }
        return columns;
    }

    public void addColumnPacketFromMeta(AtomicLong packetId, ResultSetMetaData metaData,
                                         List<ColumnPacket> columns, String catalog)
        throws SQLException {
        int columnCount = metaData.getColumnCount();
        String table = metaData.getTableName(1);
        String schema = metaData.getSchemaName(1);
        table = table != null ? table : "";
        schema = schema != null ? schema : "";

        for (int i = 1; i <= columnCount; i++) {
            String columnLabel = metaData.getColumnLabel(i);
            String columnName = metaData.getColumnName(i);
            if ("mysql".equalsIgnoreCase(schema) && "user".equalsIgnoreCase(table)
                && "name".equalsIgnoreCase(columnName)) {
                columnName = "user";
                columnLabel = "user";
            }
            ColumnPacket columnPacket = getColumnPacket(catalog, schema,
                table,
                table, columnLabel,
                columnName,
                MysqlPacket.charsetNumber,
                metaData.getColumnDisplaySize(i),
                getColumnType(metaData.getColumnTypeName(i)),
                getColumnFlags(metaData, i),
                MysqlPacket.decimals,
                (byte) packetId.getAndIncrement());
            columns.add(columnPacket);
        }
    }

    public ColumnPacket getParamColumnPacket(AtomicLong packetId) {
        return getColumnPacket("def", "", "", "",
            "?", "",
            MysqlPacket.charsetNumber, 0,
            getColumnType("VARCHAR"),
            (short) ColumnStatus.allEmpty,
            MysqlPacket.decimals,
            (byte) packetId.getAndIncrement());
    }

    public ColumnPacket getColumnPacket(String catalog,
                                        String database,
                                        String table,
                                        String originalTable,
                                        String name,
                                        String originalName,
                                        short charsetNumber,
                                        int length,
                                        byte type,
                                        short flags,
                                        byte decimals,
                                        byte packetId
                                        ) {
        ColumnPacket columnPacket =  ColumnPacket.builder()
            .catalog(catalog)
            .schema(database)
            .table(table)
            .orgTable(originalTable)
            .name(name)
            .orgName(originalName)
            .characterSet(charsetNumber)
            .columnLength(length)
            .type(type)
            .flags(flags)
            .decimals(decimals)
            .build();
        columnPacket.packetId = packetId;
        return columnPacket;
    }

    public EOFPacket getEofPacket(AtomicLong packetId) {
        EOFPacket responseEof = new EOFPacket();
        responseEof.packetId = (byte) packetId.getAndIncrement();
        responseEof.header = (byte) NativeConstants.TYPE_ID_EOF;
        responseEof.warningCount = 0;
        responseEof.statusFlags = SERVER_STATUS_AUTOCOMMIT;
        return responseEof;
    }

    public static PrepareOkPacket getPrepareOkPacket(AtomicLong packetId,
                                                     int statementId, int numberFields,
                                                     int numberParams, int warnings) {
        PrepareOkPacket packet = new PrepareOkPacket();
        packet.header = NativeConstants.TYPE_ID_OK;
        packet.packetId = (byte) packetId.getAndIncrement();
        packet.affectedRows = warnings;
        packet.statementId = statementId;
        packet.numberFields = numberFields;
        packet.numberParams = numberParams;
        return packet;
    }
}
