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
import io.dingodb.driver.mysql.MysqlType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.common.mysql.constant.ServerStatus.SERVER_STATUS_AUTOCOMMIT;

public class MysqlPacketFactory {
    private static final short BINARY_CHARSET = 63;
    private static final Charset LATIN1_CHARSET = Charset.forName("windows-1252");
    private static final short LATIN1_COLLATION = 8;
    private static final short ASCII_COLLATION = 11;
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
        long nextId = packetId.getAndIncrement();
        okPacket.packetId = (byte) nextId;
        //int status = SERVER_STATUS_AUTOCOMMIT;
        //if (serverStatus != 0) {
        //    status |= serverStatus;
        //}
        okPacket.warningCount = warningCount;
        okPacket.serverStatus = serverStatus;
        okPacket.insertId = lastInsertId;
        return okPacket;
    }

    public short getColumnFlags(ResultSetMetaData metaData, int column) {
        try {
            int columnFlags = metaData.isNullable(column) == ResultSetMetaData.columnNoNulls
                ? ColumnStatus.COLUMN_NOT_NULL : 0;
            String columnTypeName = metaData.getColumnTypeName(column);
            return (short) combineColumnFlags(columnFlags, columnTypeName);
        } catch (Exception e) {
            return 0;
        }
    }

    public static short getColumnFlags(ResultSet resultSet) {
        try {
            int columnFlags = resultSet.getInt("NULLABLE") == ResultSetMetaData.columnNoNulls
                ? ColumnStatus.COLUMN_NOT_NULL : 0;

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
                                               boolean showFields,
                                               String columnNmCharset) throws SQLException {

        List<ColumnPacket> columns = new ArrayList<>();
        String catalog = "def";
        if (showFields) {
            Charset resultsCharset = textCharset(io.dingodb.common.util.Utils.getCharacterSet(columnNmCharset));
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
                    getColumnCharsetNumber(dataType, resultsCharset),
                    resultSet.getInt("COLUMN_SIZE"),
                    getColumnType(dataType),
                    getColumnFlags(resultSet),
                    MysqlPacket.decimals,
                    (byte) packetId.getAndIncrement(),
                    resultsCharset.name()
                    );
                columns.add(columnPacket);
            }
        } else {
            addColumnPacketFromMeta(packetId, resultSet.getMetaData(), columns, catalog, columnNmCharset);
        }
        return columns;
    }

    static boolean isComputedBoolean(ResultSetMetaData metaData, int column, String typeName) throws SQLException {
        if (!"BOOLEAN".equals(typeName)) {
            return false;
        }
        String table = metaData.getTableName(column);
        return table == null || table.isEmpty();
    }

    public void addColumnPacketFromMeta(AtomicLong packetId, ResultSetMetaData metaData,
                                         List<ColumnPacket> columns, String catalog, String columnNmCharset)
        throws SQLException {
        int columnCount = metaData.getColumnCount();
        String table = metaData.getTableName(1);
        String schema = metaData.getSchemaName(1);
        table = table != null ? table : "";
        schema = schema != null ? schema : "";
        Charset resultCharset = textCharset(io.dingodb.common.util.Utils.getCharacterSet(columnNmCharset));

        for (int i = 1; i <= columnCount; i++) {
            String columnLabel = metaData.getColumnLabel(i);
            String columnName = metaData.getColumnName(i);
            if ("mysql".equalsIgnoreCase(schema) && "user".equalsIgnoreCase(table)
                && "name".equalsIgnoreCase(columnName)) {
                columnName = "user";
                columnLabel = "user";
            }
            String columnTypeName = metaData.getColumnTypeName(i);
            byte columnType = getColumnType(columnTypeName);
            if (isComputedBoolean(metaData, i, columnTypeName)) {
                // MySQL comparison expressions use integer 0/1, while declared
                // BOOLEAN columns retain TINYINT(1) for client compatibility.
                columnType = MysqlType.FIELD_TYPE_LONGLONG;
            }
            ColumnPacket columnPacket = getColumnPacket(catalog, schema,
                table,
                table, columnLabel,
                columnName,
                getColumnCharsetNumber(columnTypeName, resultCharset),
                metaData.getColumnDisplaySize(i),
                columnType,
                getColumnFlags(metaData, i),
                MysqlPacket.decimals,
                (byte) packetId.getAndIncrement(), resultCharset.name());
            columns.add(columnPacket);
        }
    }

    private static Charset textCharset(String name) throws SQLException {
        if ("UTF-8".equalsIgnoreCase(name) || "UTF8".equalsIgnoreCase(name)
            || "utf8mb4".equalsIgnoreCase(name) || "utf8mb3".equalsIgnoreCase(name)) {
            return StandardCharsets.UTF_8;
        }
        if ("windows-1252".equalsIgnoreCase(name) || "Cp1252".equalsIgnoreCase(name)
            || "latin1".equalsIgnoreCase(name)) {
            return LATIN1_CHARSET;
        }
        if ("US-ASCII".equalsIgnoreCase(name) || "ASCII".equalsIgnoreCase(name)) {
            return StandardCharsets.US_ASCII;
        }
        throw new SQLException("No MySQL collation for result charset: " + name);
    }

    private static short getColumnCharsetNumber(String typeName, Charset resultsCharset) {
        if ("VARBINARY".equals(typeName)) {
            return BINARY_CHARSET;
        }
        if ("VARCHAR".equals(typeName) || "CHAR".equals(typeName)) {
            if (resultsCharset == LATIN1_CHARSET) {
                return LATIN1_COLLATION;
            }
            return resultsCharset == StandardCharsets.US_ASCII ? ASCII_COLLATION : MysqlPacket.charsetNumber;
        }
        return MysqlPacket.charsetNumber;
    }

    /** Result text uses the connection's character_set_results, regardless of expression charset. */
    public static CharsetEncoder[] getColumnEncoders(ResultSetMetaData metaData, String connectionCharset)
        throws SQLException {
        Charset charset = textCharset(io.dingodb.common.util.Utils.getCharacterSet(connectionCharset));
        CharsetEncoder[] encoders = new CharsetEncoder[metaData.getColumnCount()];
        for (int i = 0; i < encoders.length; i++) {
            encoders[i] = charset.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return encoders;
    }

    public static byte[] encodeText(String text, CharsetEncoder encoder) throws SQLException {
        try {
            ByteBuffer encoded = encoder.encode(CharBuffer.wrap(text));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException e) {
            throw new SQLException("Text cannot be encoded as " + encoder.charset().name(), e);
        }
    }

    public ColumnPacket getParamColumnPacket(AtomicLong packetId, String columnNmCharset) {
        return getColumnPacket("def", "", "", "",
            "?", "",
            MysqlPacket.charsetNumber, 0,
            getColumnType("VARCHAR"),
            (short) ColumnStatus.allEmpty,
            MysqlPacket.decimals,
            (byte) packetId.getAndIncrement(), columnNmCharset);
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
                                        byte packetId,
                                        String columnNmCharset
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
            .columnNmCharset(columnNmCharset)
            .build();
        columnPacket.packetId = packetId;
        return columnPacket;
    }

    public static EOFPacket getEofPacket(AtomicLong packetId) {
        EOFPacket responseEof = new EOFPacket();
        responseEof.packetId = (byte) packetId.getAndIncrement();
        responseEof.header = (byte) NativeConstants.TYPE_ID_EOF;
        responseEof.warningCount = 0;
        responseEof.statusFlags = SERVER_STATUS_AUTOCOMMIT;
        return responseEof;
    }

    public static EOFPacket getEofPacket(AtomicLong packetId, int serverStatus) {
        EOFPacket responseEof = new EOFPacket();
        responseEof.packetId = (byte) packetId.getAndIncrement();
        responseEof.header = (byte) NativeConstants.TYPE_ID_EOF;
        responseEof.warningCount = 0;
        responseEof.statusFlags = serverStatus;
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
