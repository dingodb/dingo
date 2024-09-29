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

package io.dingodb.driver.mysql.process;

import io.dingodb.common.mysql.MysqlByteUtil;
import io.dingodb.common.mysql.constant.ErrorCode;
import io.dingodb.common.mysql.constant.ServerStatus;
import io.dingodb.driver.DingoConnection;
import io.dingodb.driver.DingoPreparedStatement;
import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.driver.mysql.NativeConstants;
import io.dingodb.driver.mysql.command.MysqlCommands;
import io.dingodb.driver.mysql.command.MysqlResponseHandler;
import io.dingodb.driver.mysql.packet.ExecuteStatementPacket;
import io.dingodb.driver.mysql.packet.MysqlPacketFactory;
import io.dingodb.driver.mysql.packet.OKPacket;
import io.dingodb.driver.mysql.packet.QueryPacket;
import io.dingodb.driver.mysql.packet.ResetStatementPacket;
import io.dingodb.driver.mysql.packet.SendBlobPacket;
import io.dingodb.verify.privilege.PrivilegeVerify;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.jdbc.CalciteSchema;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.calcite.operation.SetOptionOperation.CONNECTION_CHARSET;

@Slf4j
public final class MessageProcess {

    public static final MysqlCommands commands = new MysqlCommands();

    private MessageProcess() {
    }

    public static void process(ByteBuf msg, MysqlConnection mysqlConnection) {
        int length = msg.readableBytes();
        byte[] array = new byte[length];
        msg.getBytes(msg.readerIndex(), array);
        byte flg = array[1];
        byte packetIdByte = array[0];
        AtomicLong packetId = new AtomicLong(packetIdByte);
        packetId.incrementAndGet();
        String connCharSet = null;

        try {
            connCharSet = mysqlConnection.getConnection().getClientInfo(CONNECTION_CHARSET);
        } catch(SQLException e) {
            log.error("Fail to get connection characterSet, {}", e.toString());
        }

        if (flg != NativeConstants.COM_QUIT && flg != NativeConstants.COM_QUERY && mysqlConnection.passwordExpire)  {
            MysqlResponseHandler.responseError(packetId, mysqlConnection.channel, ErrorCode.ER_PASSWORD_EXPIRE, connCharSet);
            return;
        }
        switch (flg) {
            case NativeConstants.COM_QUIT:
                // quit
                if (mysqlConnection.channel.isActive()) {
                    mysqlConnection.channel.close();
                }
                break;
            case NativeConstants.COM_INIT_DB:
                // init db
                // use database
                byte[] schemaBytes = new byte[length - 2];
                System.arraycopy(array, 2, schemaBytes, 0, schemaBytes.length);
                DingoConnection connection = (DingoConnection) mysqlConnection.getConnection();
                String usedSchema = new String(schemaBytes);
                usedSchema = usedSchema.toUpperCase();
                String user = connection.getContext().getOption("user");
                String host = connection.getContext().getOption("host");
                if (!PrivilegeVerify.verify(user, host, usedSchema, null, "use")) {
                    String error =
                        String.format(ErrorCode.ER_ACCESS_DB_DENIED_ERROR.message, user, host, usedSchema);
                    MysqlResponseHandler.responseError(packetId, mysqlConnection.channel,
                        ErrorCode.ER_ACCESS_DB_DENIED_ERROR, error, connCharSet);
                    return;
                }
                // todo: current version, ignore name case
                CalciteSchema schema = connection.getContext().getRootSchema().getSubSchema(usedSchema, false);
                if (schema != null) {
                    connection.getContext().setUsedSchema(schema);
                    OKPacket okPacket = MysqlPacketFactory.getInstance().getOkPacket(0, packetId,
                        ServerStatus.SERVER_SESSION_STATE_CHANGED, null);
                    MysqlResponseHandler.responseOk(okPacket, mysqlConnection.channel);
                } else {
                    MysqlResponseHandler.responseError(packetId, mysqlConnection.channel,
                        ErrorCode.ER_NO_DATABASE_ERROR, connCharSet);
                }
                break;
            case NativeConstants.COM_QUERY:
                QueryPacket queryPacket = new QueryPacket();
                queryPacket.read(array);
                queryPacket.extendClientFlg = mysqlConnection.authPacket.extendClientFlags;
                queryPacket.clientFlg = mysqlConnection.authPacket.clientFlags;

                commands.execute(queryPacket, mysqlConnection);
                break;
            case NativeConstants.COM_FIELD_LIST:
                // show fields by tableName
                byte[] tableBytes = new byte[length - 2];
                System.arraycopy(array, 2, tableBytes, 0, tableBytes.length);
                String table = new String(tableBytes);
                MysqlCommands.executeShowFields(table, packetId, mysqlConnection);
                break;
            case NativeConstants.COM_CREATE_DB:
                // create database
                // This instruction is outdated and has been replaced by the SQL statement create database
                break;
            case NativeConstants.COM_DROP_DB:
                // drop database
                // This instruction is outdated and has been replaced by the SQL statement drop database
                break;
            case NativeConstants.COM_REFRESH:
                // clean cache
            case NativeConstants.COM_SHUTDOWN:
                // stop server
                break;
            case NativeConstants.COM_STATISTICS:
                // get server statistical information
                break;
            case NativeConstants.COM_PROCESS_INFO:
                // get current connection list
                break;
            case NativeConstants.COM_CONNECT:
                // inner thread status
                break;
            case NativeConstants.COM_PROCESS_KILL:
                // break n connection
                break;
            case NativeConstants.COM_DEBUG:
                // save server debug information
                break;
            case NativeConstants.COM_PING:
                // test ping
                OKPacket okPacket = MysqlPacketFactory.getInstance().getOkPacket(0, packetId, null);
                MysqlResponseHandler.responseOk(okPacket, mysqlConnection.channel);
                break;
            case NativeConstants.COM_TIME:
                // time
                break;
            case NativeConstants.COM_DELAYED_INSERT:
                // delayedInsert
                break;
            case NativeConstants.COM_CHANGE_USER:
                // change user
                break;
            case NativeConstants.COM_BINLOG_DUMP:
                // bin log dump
                break;
            case NativeConstants.COM_TABLE_DUMP:
                // table dump
                break;
            case NativeConstants.COM_CONNECT_OUT:
                // connect out
                break;
            case NativeConstants.COM_REGISTER_SLAVE:
                // register_slave
                break;
            case NativeConstants.COM_STMT_PREPARE:
                // prepare sql
                byte[] prepareSql = new byte[array.length - 2];
                System.arraycopy(array, 2, prepareSql, 0, prepareSql.length);
                String prepare = new String(prepareSql);
                commands.prepare(mysqlConnection, prepare);
                break;
            case NativeConstants.COM_STMT_EXECUTE:
                byte[] statementIdBytes = new byte[4];
                System.arraycopy(array, 2, statementIdBytes, 0, statementIdBytes.length);
                int statementId = MysqlByteUtil.bytesToIntLittleEndian(statementIdBytes);
                connection = (DingoConnection) mysqlConnection.getConnection();
                int paramCount;
                DingoPreparedStatement preparedStatement;
                try {
                    preparedStatement
                        = (DingoPreparedStatement) connection.getStatement(new Meta.StatementHandle(connection.id,
                        statementId, null));
                    paramCount = preparedStatement.getParameterCount();
                } catch (NoSuchStatementException e) {
                    throw new RuntimeException(e);
                }
                ExecuteStatementPacket statementPacket = new ExecuteStatementPacket(paramCount,
                    preparedStatement.getTypes());
                statementPacket.read(array);
                commands.executeStatement(statementPacket, preparedStatement, preparedStatement.getStatementType(), packetId, mysqlConnection);
                preparedStatement.setBoundTypes(statementPacket.types);
                break;
            case NativeConstants.COM_STMT_SEND_LONG_DATA:
                // send blob data
                SendBlobPacket blobPacket = new SendBlobPacket();
                blobPacket.read(array);
                statementId = blobPacket.getStatementId();
                connection = (DingoConnection) mysqlConnection.getConnection();
                try {
                    preparedStatement
                        = (DingoPreparedStatement) connection.getStatement(new Meta.StatementHandle(connection.id,
                        statementId, null));
                    preparedStatement.setBytes(blobPacket.getParameter() + 1,
                        blobPacket.getPayload());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                break;
            case NativeConstants.COM_STMT_CLOSE:
                // statement close
                statementIdBytes = new byte[4];
                System.arraycopy(array, 2, statementIdBytes, 0, statementIdBytes.length);
                statementId = MysqlByteUtil.bytesToIntLittleEndian(statementIdBytes);
                connection = (DingoConnection) mysqlConnection.getConnection();
                try {
                    preparedStatement
                        = (DingoPreparedStatement) connection.getStatement(new Meta.StatementHandle(connection.id,
                        statementId, null));
                    preparedStatement.close();
                } catch (NoSuchStatementException e) {
                    throw new RuntimeException(e);
                } catch (SQLException e) {
                    MysqlResponseHandler.responseError(packetId, mysqlConnection.channel, e, connCharSet);
                }
                break;
            case NativeConstants.COM_STMT_RESET:
                // destroy prepare sql param cache  : statement reset
                ResetStatementPacket reset = new ResetStatementPacket();
                reset.read(array);
                connection = (DingoConnection) mysqlConnection.getConnection();
                try {
                    preparedStatement
                        = (DingoPreparedStatement) connection.getStatement(new Meta.StatementHandle(connection.id,
                        reset.getStatementId(), null));
                    preparedStatement.clearParameters();
                } catch (NoSuchStatementException e) {
                    throw new RuntimeException(e);
                } catch (SQLException e) {
                    MysqlResponseHandler.responseError(packetId, mysqlConnection.channel, e, connCharSet);
                }
                okPacket = MysqlPacketFactory.getInstance().getOkPacket(0, packetId, null);
                MysqlResponseHandler.responseOk(okPacket, mysqlConnection.channel);
                break;
            case NativeConstants.COM_SET_OPTION:
                // set option
                break;
            case NativeConstants.COM_STMT_FETCH:
                // fetch prepare statement result
                break;
            case NativeConstants.COM_DAEMON:
                // daemon
                break;
            case NativeConstants.COM_BINLOG_DUMP_GTID:
                // binlog dump
                break;
            case NativeConstants.COM_RESET_CONNECTION:
                // reset connection
                break;
            case 0x20:
                // end
                break;
            default:
                break;
        }
    }
}
