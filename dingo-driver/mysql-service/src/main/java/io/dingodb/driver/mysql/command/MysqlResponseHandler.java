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

import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.MemoryManager;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.memory.MemoryPoolUtils;
import io.dingodb.common.memory.MemoryType;
import io.dingodb.common.memory.ObjectSizeUtils;
import io.dingodb.common.mysql.ExtendedClientCapabilities;
import io.dingodb.common.mysql.MysqlServer;
import io.dingodb.common.mysql.State;
import io.dingodb.common.mysql.constant.ServerStatus;
import io.dingodb.common.mysql.error.ErrorMessage;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.driver.DingoConnection;
import io.dingodb.driver.common.DingoArray;
import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.driver.mysql.netty.AsyncStreamReader;
import io.dingodb.driver.mysql.packet.ColumnPacket;
import io.dingodb.driver.mysql.packet.ColumnsNumberPacket;
import io.dingodb.driver.mysql.packet.EOFPacket;
import io.dingodb.driver.mysql.packet.ERRPacket;
import io.dingodb.driver.mysql.packet.MysqlPacketFactory;
import io.dingodb.driver.mysql.packet.OKPacket;
import io.dingodb.driver.mysql.packet.PreparePacket;
import io.dingodb.driver.mysql.packet.PrepareResultSetRowPacket;
import io.dingodb.driver.mysql.packet.ResultSetRowPacket;
import io.dingodb.exec.memory.MemoryController;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.calcite.executor.SetOptionExecutor.CONNECTION_CHARSET;
import static io.dingodb.common.mysql.constant.ServerStatus.SERVER_MORE_RESULTS_EXISTS;
import static io.dingodb.common.mysql.error.ErrorCode.ErrRecursiveCteErr;
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
        String connCharSet = null;
        try {
            connCharSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
            List<ColumnPacket> columnPackets = factory.getColumnPackets(packetId, resultSet, true, connCharSet);
            ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
            for (ColumnPacket columnPacket : columnPackets) {
                columnPacket.write(buffer);
            }
            OKPacket okPacket = factory.getOkEofPacket(0, packetId, ServerStatus.SERVER_STATUS_AUTOCOMMIT);
            okPacket.write(buffer);
            mysqlConnection.writeAndFlushImmediately(buffer);
        } catch (SQLException e) {
            responseError(packetId, mysqlConnection, e, connCharSet);
        }
    }

    public static boolean responseResultSet(ResultSet resultSet,
                                         AtomicLong packetId,
                                   MysqlConnection mysqlConnection,
                                   boolean hasMore,
                                   Statement statement) throws SQLException {
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
        String connCharSet = null;
        boolean stream = false;
        try {
            connCharSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
            ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
            ResultSetMetaData metaData = resultSet.getMetaData();
            ColumnsNumberPacket columnsNumberPacket = new ColumnsNumberPacket();
            columnsNumberPacket.packetId = (byte) packetId.getAndIncrement();
            columnsNumberPacket.columnsNumber = metaData.getColumnCount();
            columnsNumberPacket.write(buffer);

            List<ColumnPacket> columns = factory.getColumnPackets(packetId, resultSet, false, connCharSet);
            for (ColumnPacket columnPacket : columns) {
                columnPacket.write(buffer);
            }

            int initServerStatus = getInitServerStatus((DingoConnection) mysqlConnection.getConnection());
            if (hasMore) {
                initServerStatus |= SERVER_MORE_RESULTS_EXISTS;
            }
            if (deprecateEof) {
                stream = handlerRowPacket(
                    resultSet, packetId, mysqlConnection, buffer, initServerStatus, statement
                );
                if (!stream) {
                    OKPacket okEofPacket = factory.getOkEofPacket(
                        0, packetId, initServerStatus
                    );
                    okEofPacket.write(buffer);
                }
            } else {
                // intermediate eof
                EOFPacket eofPacket = MysqlPacketFactory.getEofPacket(packetId);
                if (hasMore) {
                    eofPacket.statusFlags |= SERVER_MORE_RESULTS_EXISTS;
                }
                eofPacket.write(buffer);
                // row packet...
                stream = handlerRowPacket(
                    resultSet, packetId, mysqlConnection, buffer, initServerStatus, statement
                );
                if (!stream) {
                    // response EOF
                    //resultSetPacket.rowsEof = getEofPacket(packetId);
                    eofPacket = MysqlPacketFactory.getEofPacket(packetId, initServerStatus);
                    eofPacket.write(buffer);
                }
            }
            if (!stream) {
                mysqlConnection.writeAndFlushImmediately(buffer);
            }
        } catch (SQLException e) {
            responseError(packetId, mysqlConnection, e, connCharSet);
        } finally {
            if (!stream) {
                resultSet.close();
            }
        }
        return stream;
    }

    private static Boolean handlerRowPacket(
        ResultSet resultSet,
        AtomicLong packetId,
        MysqlConnection mysqlConnection,
        ByteBuf buffer,
        int serverStatus,
        Statement statement
    ) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        String typeName;
        boolean stream = false;
        AtomicLong cnt = new AtomicLong(0);
        AtomicLong dataSize = new AtomicLong(0);
        MemoryController memoryController = null;
        while (resultSet.next()) {
            ResultSetRowPacket resultSetRowPacket = new ResultSetRowPacket();
            long nextId = packetId.getAndIncrement();
            resultSetRowPacket.packetId = (byte) nextId;
            String characterSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
            characterSet = getCharacterSet(characterSet);
            resultSetRowPacket.setCharacterSet(characterSet);
            for (int i = 1; i <= columnCount; i ++) {
                Object val = resultSet.getObject(i);
                dataSize.addAndGet(ObjectSizeUtils.calculateObjectSize(val));
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
                    if(val != null) {
                        val = "0x" + Long.toHexString((long)val);
                    }
                }

                resultSetRowPacket.addColumnValue(val);
            }
            cnt.incrementAndGet();
            resultSetRowPacket.write(buffer);
            if (dataSize.get() > 4 * 1024 * 1024) {
                if (memoryController == null) {
                    MemoryPool childCachePool = MemoryPoolUtils.createCacheTmpTablePool(
                        "protocol" + UUID.randomUUID(), MemoryManager.getInstance().getCacheMemoryPool());
                    memoryController = new MemoryController(childCachePool);
                }
                LogUtils.info(log, "[memory] protocol cache allocate size:{}", dataSize.get());
                memoryController.allocate(dataSize.get());
                dataSize.set(0);
            }
            if (cnt.get() % ScopeVariables.getMysqlStreamSize() == 0) {
                LogUtils.info(log, "write big data. "
                    + " cnt:{}, packetId:{}",
                    cnt.get(), packetId.get());
                AsyncStreamReader streamReader = new AsyncStreamReader(
                    resultSet, packetId, mysqlConnection, serverStatus, statement
                );
                Executors.submit("streamReader", streamReader);
                mysqlConnection.writeAndFlushByStream(buffer);
                stream = true;
                break;
            }
        }
        if (memoryController != null) {
            memoryController.close();
        }
        return stream;
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
            // arrayVal = getDateByTimezone(arrayVal, dingoConnection.getTimeZone());
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
                                     MysqlConnection mysqlConnection,
                                     int errorCode,
                                     String message,
                                     String characterSet) {
        responseError(packetId, mysqlConnection, errorCode, State.mysqlState.getOrDefault(errorCode, "HY000"),
             message, characterSet);
    }

    public static void responseError(AtomicLong packetId,
                                     MysqlConnection mysqlConnection,
                                     int errorCode,
                                     String characterSet) {
        responseError(packetId, mysqlConnection, errorCode, State.mysqlState.getOrDefault(errorCode, "HY000"),
            ErrorMessage.errorMap.getOrDefault(errorCode, "Unknown error"), characterSet);
    }

    public static void responseError(AtomicLong packetId,
                                     MysqlConnection mysqlConnection,
                                     int errorCode,
                                     String sqlState,
                                     String message,
                                     String characterSet) {
        ERRPacket ep = new ERRPacket();
        ep.packetId = (byte) packetId.getAndIncrement();
        ep.capabilities = MysqlServer.getServerCapabilities();
        ep.errorCode = errorCode;
        ep.sqlState = sqlState;
        ep.errorMessage = message;
        if ("utf8mb4".equalsIgnoreCase(characterSet) || "utf8mb3".equalsIgnoreCase(characterSet)) {
            characterSet = "utf8";
        }
        ep.characterSet = characterSet;
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        ep.write(buffer);
        mysqlConnection.writeAndFlushImmediately(buffer);
    }

    public static void responseError(AtomicLong packetId,
                                     MysqlConnection mysqlConnection,
                                     SQLException exception,
                                     String characterSet) {
        exception = errorDingo2Mysql(exception);
        ERRPacket ep = new ERRPacket();
        ep.packetId = (byte) packetId.getAndIncrement();
        ep.capabilities = MysqlServer.getServerCapabilities();
        ep.errorCode = exception.getErrorCode();
        ep.sqlState = exception.getSQLState();
        ep.characterSet = characterSet;

        if (exception.getMessage() == null) {
            ep.errorMessage = "";
        } else if (exception.getMessage().startsWith("Encountered")) {
            String errorDetail = exception.getMessage();
            String subErr = getSubErr(errorDetail);
            ep.errorMessage = "You have an error in your SQL syntax; check the manual that corresponds "
                + "to your MySQL server version for the right syntax to use near " + subErr;
        } else {
            ep.errorMessage = exception.getMessage();
        }
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        ep.write(buffer);
        mysqlConnection.writeAndFlushImmediately(buffer);
    }

    @Nullable
    private static String getSubErr(String errorDetail) {
        try {
            int lx = errorDetail.indexOf("Was expecting one of");
            int sx = errorDetail.indexOf("Encountered");
            String subErr = null;
            if (sx > -1 && lx > sx) {
                subErr = errorDetail.substring(sx, lx);
                sx = subErr.indexOf("> \"");
                if (sx > -1) {
                    if (subErr.length() > (sx + 3)) {
                        subErr = subErr.substring(sx + 3);
                    }
                    subErr = subErr.replace("\"\"", "");
                    sx = subErr.lastIndexOf(".");
                    if (sx > 0) {
                        subErr = subErr.substring(0, sx);
                    }
                }
            }
            return subErr;
        } catch (Exception e) {
            return "";
        }
    }

    public static SQLException errorDingo2Mysql(SQLException e) {
        if (e.getMessage() == null) {
            return e;
        }
        if (e.getMessage() != null && e.getMessage().contains("Duplicate data")) {
            return new SQLException("Duplicate data for key 'PRIMARY'", "23000", 1062);
        } else if (e.getMessage() != null && (e.getMessage().contains("TaskCancelException")
            || e.getMessage().contains("task is cancel"))) {
            return new SQLException("Query execution was interrupted", "70100", 1317);
        } else if (e.getMessage() != null && e.getMessage().contains("Statement canceled")) {
            LogUtils.info(log, e.getMessage(), e);
            return new SQLException(e.getMessage(), "HY000", 1105);
        } else if (e.getErrorCode() == 9001 && e.getSQLState().equals("45000")) {
            int code = 1105;
            String state = "HY000";
            String reason = e.getMessage();
            if (reason.contains("Duplicate entry")) {
                code = 1062;
                state = "23000";
            }
            if (reason.contains("java.lang.RuntimeException:")) {
                reason = reason.replace("java.lang.RuntimeException:", "");
            }
            return new SQLException(reason, state, code);
        } else if (e.getErrorCode() == 5001 && e.getSQLState().equals("45000")) {
            if (e.getMessage().contains("Syntax Error")) {
                return new SQLException(
                    e.getMessage(),
                    e.getSQLState(),
                    e.getErrorCode());
            } else if (e.getMessage().contains("io.dingodb.store.api.transaction.exception.DuplicateEntryException:")) {
                return new SQLException(e.getMessage()
                    .replace("io.dingodb.store.api.transaction.exception.DuplicateEntryException:", ""),
                     "23000", 1062);
            } else if (e.getMessage().contains("Recursive query aborted after")) {
                String err = e.getMessage().replace("java.lang.RuntimeException:", "").trim();
                return new SQLException(err, "HY000", ErrRecursiveCteErr);
            }
            return e;
        } else if (e.getErrorCode() == 1105) {
            String reason = e.getMessage();
            if (reason.contains("java.lang.RuntimeException:")) {
                reason = reason.replace("java.lang.RuntimeException:", "");
            }
            return new SQLException(reason, e.getSQLState(), e.getErrorCode());
        } else {
            return e;
        }
    }

    public static void responseOk(OKPacket okPacket, MysqlConnection mysqlConnection) {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        okPacket.write(buffer);
        mysqlConnection.writeAndFlushImmediately(buffer);
    }

    public static void responseOk(MysqlConnection mysqlConnection) {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        buffer.writeBytes(OKPacket.OK);
        mysqlConnection.writeAndFlushImmediately(buffer);
    }

    public static void responsePrepare(PreparePacket preparePacket, MysqlConnection mysqlConnection) {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        preparePacket.write(buffer);
        mysqlConnection.writeAndFlushImmediately(buffer);
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
        String connCharSet = null;
        try {
            connCharSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
            ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
            ResultSetMetaData metaData = resultSet.getMetaData();
            ColumnsNumberPacket columnsNumberPacket = new ColumnsNumberPacket();
            columnsNumberPacket.packetId = (byte) packetId.getAndIncrement();
            int columnCount = metaData.getColumnCount();
            columnsNumberPacket.columnsNumber = columnCount;
            columnsNumberPacket.write(buffer);

            List<ColumnPacket> columns = factory.getColumnPackets(packetId, resultSet, false, connCharSet);
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
                MysqlPacketFactory.getEofPacket(packetId).write(buffer);
                // row packet...
                handlerPrepareRowPacket(resultSet, packetId, mysqlConnection, buffer, columnCount);
                // response EOF
                //resultSetPacket.rowsEof = getEofPacket(packetId);
                MysqlPacketFactory.getEofPacket(packetId).write(buffer);
            }
            mysqlConnection.writeAndFlushImmediately(buffer);
        } catch (SQLException e) {
            responseError(packetId, mysqlConnection, e, connCharSet);
        }
    }
}
