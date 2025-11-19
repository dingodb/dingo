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

import io.dingodb.common.exception.DingoSqlException;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.mysql.ExtendedClientCapabilities;
import io.dingodb.common.mysql.MysqlByteUtil;
import io.dingodb.common.mysql.constant.ServerStatus;
import io.dingodb.common.parser.ByteString;
import io.dingodb.driver.DingoConnection;
import io.dingodb.driver.DingoPreparedStatement;
import io.dingodb.driver.DingoStatement;
import io.dingodb.driver.mysql.MultiStatementSplitter;
import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.driver.mysql.MysqlType;
import io.dingodb.driver.mysql.packet.ColumnPacket;
import io.dingodb.driver.mysql.packet.EOFPacket;
import io.dingodb.driver.mysql.packet.ExecuteStatementPacket;
import io.dingodb.driver.mysql.packet.LoadDataResPacket;
import io.dingodb.driver.mysql.packet.MysqlPacketFactory;
import io.dingodb.driver.mysql.packet.OKPacket;
import io.dingodb.driver.mysql.packet.PrepareOkPacket;
import io.dingodb.driver.mysql.packet.PreparePacket;
import io.dingodb.driver.mysql.packet.QueryPacket;
import io.dingodb.exec.transaction.base.ITransaction;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Meta;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.calcite.executor.SetOptionExecutor.CONNECTION_CHARSET;
import static io.dingodb.common.mysql.constant.ServerStatus.SERVER_MORE_RESULTS_EXISTS;
import static io.dingodb.common.mysql.error.ErrorCode.ErrMustChangePassword;
import static io.dingodb.common.mysql.error.ErrorCode.ErrParse;
import static io.dingodb.common.mysql.error.ErrorCode.ErrUnknown;
import static io.dingodb.common.util.Utils.getCharacterSet;

@Slf4j
public class MysqlCommands {

    public static final String setPwdSqlTemp1 = "set password for %s@%s";
    public static final String setPwdSqlTemp2 = "set password for %s =";

    public static final String alterUserPwdSqlTemp1 = "alter user %s@%s identified by";
    public static final String alterUserPwdSqlTemp2 = "alter user %s identified by";

    MysqlPacketFactory mysqlPacketFactory = MysqlPacketFactory.getInstance();

    public static void executeShowFields(String table, AtomicLong packetId, MysqlConnection mysqlConnection) {
        String connCharSet = null;
        try {
            connCharSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
            ResultSet rs = mysqlConnection.getConnection().getMetaData().getColumns(null, null,
                table, null);
            MysqlResponseHandler.responseShowField(rs, packetId, mysqlConnection);
        } catch (SQLException e) {
            MysqlResponseHandler.responseError(packetId, mysqlConnection, e, connCharSet);
        }
    }

    public void execute(QueryPacket queryPacket,
                        MysqlConnection mysqlConnection) {
        ByteString sql;
        String characterSet;
        try {
            characterSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
            characterSet = getCharacterSet(characterSet);
            sql = new ByteString(queryPacket.message, characterSet);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //AtomicLong packetId = new AtomicLong(queryPacket.packetId + 1);
        //if (mysqlConnection.passwordExpire && !doExpire(mysqlConnection, sql, packetId)) {
        //    MysqlResponseHandler.responseError(packetId, mysqlConnection.channel, ErrorCode.ER_PASSWORD_EXPIRE,
        //        characterSet);
        //    return;
        //}
        AtomicLong packetId = new AtomicLong(queryPacket.packetId + 1);
        List<ByteString> statements;
        try {
            MultiStatementSplitter splitter = new MultiStatementSplitter(sql);
            statements = splitter.split();
        } catch (Exception e) {
            MysqlResponseHandler.responseError(packetId, mysqlConnection,
                ErrParse, "You have an error in your SQL syntax", characterSet);
            return;
        }
        if (statements.isEmpty()) {
            // write ok package
            MysqlResponseHandler.responseOk(mysqlConnection);
            return;
        }
        for (int i = 0; i < statements.size(); i ++) {
            if (!executeStatement(statements.get(i), packetId, mysqlConnection,
             i < statements.size() - 1, characterSet)) {
                break;
            }
        }
    }

    private static boolean doExpire(MysqlConnection mysqlConnection, String sql, AtomicLong packetId) {
        sql = sql.trim().toLowerCase().replace("'", "");
        DingoConnection dingoConnection = (DingoConnection) mysqlConnection.getConnection();
        String user = dingoConnection.getContext().getOption("user");
        String host = dingoConnection.getContext().getOption("host");
        String setPwdSql1 = String.format(setPwdSqlTemp1, user, host);
        String alterUserPwdSql1 = String.format(alterUserPwdSqlTemp1, user, host);
        String setPwdSql2;
        String alterUserPwdSql2;
        if (host.contains("%")) {
            setPwdSql2 = String.format(setPwdSqlTemp2, user);
            alterUserPwdSql2 = String.format(alterUserPwdSqlTemp2, user);
            if (sql.startsWith(setPwdSql2) || sql.startsWith(alterUserPwdSql2)) {
                return true;
            }
        }

        try {
            if (sql.startsWith(setPwdSql1) || sql.startsWith(alterUserPwdSql1)) {
                MysqlResponseHandler.responseError(packetId, mysqlConnection,
                    ErrMustChangePassword, mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET));
                return true;
            }
        } catch (SQLException e) {
            LogUtils.info(log, e.getMessage(), e);
        }
        return false;
    }

    public void prepare(MysqlConnection mysqlConnection, String sql) {
        DingoConnection connection = (DingoConnection) mysqlConnection.getConnection();
        AtomicLong packetId = new AtomicLong(2);
        String connCharSet = null;

        try {
            DingoPreparedStatement preparedStatement = (DingoPreparedStatement) connection
                .prepareStatement(sql);
            connCharSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
            Meta.StatementHandle statementHandle = preparedStatement.handle;
            String placeholder = "?";
            int i = 0;
            int numberParams = 0;
            List<ColumnPacket> paramColumnPackets = new ArrayList<>();
            while (sql.indexOf(placeholder, i) >= 0) {
                numberParams++;
                i = sql.indexOf(placeholder, i) + placeholder.length();

                paramColumnPackets.add(mysqlPacketFactory.getParamColumnPacket(packetId, connCharSet));
            }
            boolean deprecateEof = (mysqlConnection.authPacket.extendClientFlags
                & ExtendedClientCapabilities.CLIENT_DEPRECATE_EOF) != 0;
            EOFPacket intermediate = null;
            EOFPacket eofResponse = null;
            List<ColumnPacket> fieldColumnPackets = new ArrayList<>();
            int numberFields = 0;
            if (preparedStatement.getStatementType() == Meta.StatementType.SELECT) {
                if (!deprecateEof) {
                    intermediate = MysqlPacketFactory.getEofPacket(packetId);
                }
                numberFields = statementHandle.signature.columns.size();
                mysqlPacketFactory.addColumnPacketFromMeta(packetId, preparedStatement.getMetaData(),
                    fieldColumnPackets, "def", connCharSet);
            }
            if (!deprecateEof) {
                eofResponse = MysqlPacketFactory.getEofPacket(packetId);
            }

            PrepareOkPacket prepareOkPacket = MysqlPacketFactory
                .getPrepareOkPacket(new AtomicLong(1),
                    statementHandle.id, numberFields, numberParams, 0);

            PreparePacket preparePacket = PreparePacket.builder()
                .prepareOkPacket(prepareOkPacket)
                .paramColumnPackets(paramColumnPackets)
                .fieldsColumnPackets(fieldColumnPackets)
                .deprecateEof(deprecateEof)
                .intermediate(intermediate)
                .eofResponse(eofResponse)
                .build();
            MysqlResponseHandler.responsePrepare(preparePacket, mysqlConnection);
        } catch (SQLException e) {
            LogUtils.info(log, e.getMessage(), e);
            MysqlResponseHandler.responseError(packetId, mysqlConnection, e, connCharSet);
        }
    }

    public boolean executeStatement(
        ByteString sql,
        AtomicLong packetId,
        MysqlConnection mysqlConnection,
        boolean hasMore,
        String charsetStr
    ) {
        Statement statement = null;
        boolean hasResults;
        String connCharSet = null;
        long originPacketId = packetId.get();
        boolean stream = false;
        try {
            String sqlSample = sql.substring(0);
            if (mysqlConnection.passwordExpire && !doExpire(mysqlConnection, sqlSample, packetId)) {
                MysqlResponseHandler.responseError(packetId, mysqlConnection, ErrMustChangePassword,
                    charsetStr);
                return false;
            }
            statement = mysqlConnection.getConnection().createStatement();
            connCharSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
            hasResults = statement.execute(sqlSample);
            if (hasResults) {
                // select
                do {
                    ResultSet rs = statement.getResultSet();
                    stream = MysqlResponseHandler.responseResultSet(rs, packetId, mysqlConnection, hasMore, statement);
                }
                while (!stream && getMoreResults(statement));
            } else {
                // update/insert/delete/ddl
                SQLWarning sqlWarning = statement.getWarnings();
                if (sqlWarning == null) {
                    sqlWarning = mysqlConnection.getConnection().getWarnings();
                    if (sqlWarning != null && sqlWarning.getMessage().contains("local infile:")) {
                        mysqlConnection.querySpecial = true;
                        mysqlConnection.querySpecialId = sqlWarning.getSQLState();
                        String ret = sqlWarning.getMessage().substring(13);
                        LoadDataResPacket loadDataResPacket = new LoadDataResPacket();
                        loadDataResPacket.message = ret;
                        loadDataResPacket.packetId = (byte) packetId.getAndIncrement();
                        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
                        loadDataResPacket.write(buffer);
                        mysqlConnection.writeAndFlushImmediately(buffer);
                        mysqlConnection.getConnection().clearWarnings();
                        return true;
                    }
                }
                try {
                    mysqlConnection.getConnection().clearWarnings();
                } catch (SQLException e) {
                    LogUtils.error(log, e.getMessage(), e);
                }
                DingoStatement dingoStatement = (DingoStatement) statement;
                OKPacket okPacket;
                int initServerStatus = getInitServerStatus((DingoConnection) mysqlConnection.getConnection());
                if (hasMore) {
                    initServerStatus |= SERVER_MORE_RESULTS_EXISTS;
                }
                int count = statement.getUpdateCount();
                if (dingoStatement.isHasIncId()) {
                    Long lastInsertId = dingoStatement.getAutoIncId();
                    okPacket = MysqlPacketFactory.getInstance()
                        .getOkPacket(
                            count, packetId, initServerStatus, new BigInteger(String.valueOf(lastInsertId)), sqlWarning
                        );
                } else {
                    okPacket = MysqlPacketFactory.getInstance()
                        .getOkPacket(count, packetId, initServerStatus, BigInteger.ZERO, sqlWarning);
                }
                MysqlResponseHandler.responseOk(okPacket, mysqlConnection);
            }
            return true;
        } catch (SQLException sqlException) {
            LogUtils.error(log, "sql exception sqlstate:" + sqlException.getSQLState() + ", code:"
                + sqlException.getErrorCode()
                + ", message:" + sqlException.getMessage());
            MysqlResponseHandler.responseError(packetId, mysqlConnection, sqlException, connCharSet);
            return false;
        } catch (DingoSqlException sqlException) {
            LogUtils.error(log, "sql exception sqlstate:" + sqlException.getSqlState() + ", code:"
                + sqlException.getSqlCode()
                + ", message:" + sqlException.getMessage());
            if (sqlException.getMessage().contains("Recursive query aborted after")) {
                packetId.set(originPacketId);
            }
            SQLException sqlException1 =
                new SQLException(sqlException.getMessage(), sqlException.getSqlState(), sqlException.getSqlCode());
            MysqlResponseHandler.responseError(packetId, mysqlConnection, sqlException1, connCharSet);
            return false;
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            throw e;
        } finally {
            try {
                if (!stream) {
                    if (statement != null) {
                        statement.close();
                    }
                }
            } catch (SQLException e) {
                LogUtils.error(log, e.getMessage(), e);
            }
        }
    }

    private boolean getMoreResults(Statement statement) {
        try {
            return statement.getMoreResults();
        } catch (Throwable t) {
            return false;
        }
    }

    public void executeStatement(ExecuteStatementPacket statementPacket,
                                 DingoPreparedStatement preparedStatement,
                                 Meta.StatementType statementType,
                                 AtomicLong packetId,
                                 MysqlConnection mysqlConnection
    ) {
        String connectionCharSet = null;
        try {
            connectionCharSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
            statementPacket.paramValMap.forEach((k, v) -> {
                String connCharSet = null;
                try {
                    connCharSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
                    switch (v.getType()) {
                        case MysqlType.FIELD_TYPE_TINY:
                            byte byteVal = v.getValue()[0];
                            if (byteVal == 0x00) {
                                preparedStatement.setBoolean(k, false);
                            } else if (byteVal == 0x01) {
                                preparedStatement.setBoolean(k, true);
                            } else {
                                preparedStatement.setObject(k, byteVal, java.sql.Types.TINYINT);
                            }
                            break;
                        case MysqlType.FIELD_TYPE_SHORT:
                            short shortValue = ByteBuffer.wrap(v.getValue())
                                .order(ByteOrder.LITTLE_ENDIAN)
                                .getShort();
                            preparedStatement.setShort(k, shortValue);
                            break;
                        case MysqlType.FIELD_TYPE_LONGLONG:
                            long longVal = MysqlByteUtil.bytesToLongLittleEndian(v.getValue());
                            preparedStatement.setLong(k, longVal);
                            break;
                        case MysqlType.FIELD_TYPE_LONG:
                            // 4 bytes
                            // int in mysql jdbc long
                            int intVal = MysqlByteUtil.bytesToIntLittleEndian(v.getValue());
                            preparedStatement.setInt(k, intVal);
                            break;
                        case MysqlType.FIELD_TYPE_FLOAT:
                            ByteBuffer buffer = ByteBuffer.wrap(v.getValue());
                            buffer.order(ByteOrder.LITTLE_ENDIAN);
                            float floatVal = buffer.getFloat();
                            preparedStatement.setFloat(k, floatVal);
                            break;
                        case MysqlType.FIELD_TYPE_DOUBLE:
                            buffer = ByteBuffer.wrap(v.getValue());
                            buffer.order(ByteOrder.LITTLE_ENDIAN);
                            double doubleVal = buffer.getDouble();
                            preparedStatement.setDouble(k, doubleVal);
                            break;
                        case MysqlType.FIELD_TYPE_DATE:
                            long timestamp = MysqlByteUtil.bytesToDateLittleEndian(v.getValue());
                            Date dateVal = new Date(timestamp);
                            preparedStatement.setDate(k, dateVal);
                            break;
                        case MysqlType.FIELD_TYPE_TIME:
                            Time time = MysqlByteUtil.bytesToTimeLittleEndian(v.getValue());
                            preparedStatement.setTime(k, time);
                            break;
                        case MysqlType.FIELD_TYPE_DATETIME:
                        case MysqlType.FIELD_TYPE_TIMESTAMP:
                            Timestamp timeStamp = MysqlByteUtil.bytesToTimeStampLittleEndian(v.getValue());
                            preparedStatement.setTimestamp(k, timeStamp);
                            break;
                        case MysqlType.FIELD_TYPE_VAR_STRING:
                        case MysqlType.FIELD_TYPE_STRING:
                        case MysqlType.FIELD_TYPE_VARCHAR:
                            String characterSet = mysqlConnection.getConnection()
                                .getClientInfo(CONNECTION_CHARSET);
                            characterSet = getCharacterSet(characterSet);
                            String charVal;
                            try {
                                charVal = new String(v.getValue(), characterSet);
                            } catch (UnsupportedEncodingException e) {
                                throw new RuntimeException(e);
                            }
                            preparedStatement.setString(k, charVal);
                            break;
                        case MysqlType.FIELD_TYPE_DECIMAL:
                        case MysqlType.FIELD_TYPE_NEWDECIMAL:
                            String strVal = new String(v.getValue());
                            BigDecimal bigDecimal = new BigDecimal(strVal);
                            preparedStatement.setBigDecimal(k, bigDecimal);
                            break;
                        default:
                            charVal = new String(v.getValue());
                            preparedStatement.setObject(k, charVal);
                    }
                } catch (SQLException e) {
                    MysqlResponseHandler.responseError(packetId, mysqlConnection, e, connCharSet);
                }
            });
            if (statementType == Meta.StatementType.SELECT) {
                String connCharSet = null;
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    connCharSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
                    MysqlResponseHandler.responsePrepareExecute(resultSet, packetId, mysqlConnection);
                } catch (SQLException e) {
                    LogUtils.error(log, e.getMessage(), e);
                    MysqlResponseHandler.responseError(packetId, mysqlConnection, e, connCharSet);
                }
            } else if (statementType == Meta.StatementType.OTHER_DDL) {
                OKPacket okPacket = mysqlPacketFactory.getOkPacket(0, packetId, null);
                MysqlResponseHandler.responseOk(okPacket, mysqlConnection);
            } else {
                SQLWarning sqlWarning = preparedStatement.getWarnings();
                int affected = preparedStatement.executeUpdate();
                DingoConnection connection = (DingoConnection) mysqlConnection.getConnection();
                int initServerStatus = getInitServerStatus(connection);
                OKPacket okPacket;
                if (preparedStatement.isHasIncId()) {
                    Long lastInsertId = preparedStatement.getAutoIncId();
                    okPacket = MysqlPacketFactory.getInstance()
                        .getOkPacket(affected, packetId, initServerStatus, new BigInteger(String.valueOf(lastInsertId)),
                            sqlWarning);
                } else {
                    okPacket = mysqlPacketFactory.getOkPacket(affected, packetId, sqlWarning);
                }
                MysqlResponseHandler.responseOk(okPacket, mysqlConnection);
            }
        } catch (SQLException e) {
            MysqlResponseHandler.responseError(packetId, mysqlConnection, e, connectionCharSet);
        } catch (Exception e) {
            MysqlResponseHandler.responseError(
                packetId, mysqlConnection, ErrUnknown, connectionCharSet
            );
            LogUtils.error(log, e.getMessage(), e);
        }
    }

    public static int getInitServerStatus(DingoConnection connection) {
        ITransaction transaction = connection.getTransaction();
        boolean autoCommit = true;
        if (transaction != null) {
            autoCommit = !("off".equalsIgnoreCase(connection.getClientInfo("autocommit")));
        }
        int initServerStatus = 0;
        if (!autoCommit && !connection.isSessionStateChanged()) {
            initServerStatus = ServerStatus.SERVER_STATUS_IN_TRANS;
        }
        if (connection.getAutoCommit()) {
            initServerStatus |= ServerStatus.SERVER_STATUS_AUTOCOMMIT;
        }
        String tranReadOnly = connection.getClientInfo("transaction_read_only");
        tranReadOnly = tranReadOnly == null ? "off" : tranReadOnly;
        boolean txReadOnly = tranReadOnly.equalsIgnoreCase("on");
        if (txReadOnly) {
            initServerStatus |= ServerStatus.SERVER_STATUS_IN_TRANS_READONLY;
        }
        if (connection.isSessionStateChanged()) {
            initServerStatus |= ServerStatus.SERVER_SESSION_STATE_CHANGED;
            connection.setSessionStateChanged(false);
        }
        return initServerStatus;
    }

}
