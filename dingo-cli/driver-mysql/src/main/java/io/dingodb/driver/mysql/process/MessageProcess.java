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
import io.dingodb.common.mysql.MysqlMessage;
import io.dingodb.common.mysql.constant.ServerStatus;
import io.dingodb.driver.DingoConnection;
import io.dingodb.driver.DingoPreparedStatement;
import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.driver.mysql.NativeConstants;
import io.dingodb.driver.mysql.command.DingoCommands;
import io.dingodb.driver.mysql.command.MysqlResponseHandler;
import io.dingodb.driver.mysql.packet.ExecuteStatementPacket;
import io.dingodb.driver.mysql.packet.MysqlPacketFactory;
import io.dingodb.driver.mysql.packet.OKPacket;
import io.dingodb.driver.mysql.packet.QueryPacket;
import io.dingodb.driver.mysql.util.BufferUtil;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.jdbc.CalciteSchema;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MessageProcess {

    public static final DingoCommands commands = new DingoCommands();

    public static void process(ByteBuf msg, MysqlConnection mysqlConnection) {
        int length = msg.readableBytes();
        byte[] array = new byte[length];
        msg.getBytes(msg.readerIndex(), array);
        byte flg = array[1];
        byte packetIdByte = array[0];
        AtomicLong packetId = new AtomicLong(packetIdByte);
        packetId.incrementAndGet();
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
                CalciteSchema schema = connection.getContext().getRootSchema().getSubSchema(usedSchema, true);
                connection.getContext().setUsedSchema(schema);
                OKPacket okPacket = MysqlPacketFactory.getInstance().getOkPacket(0, packetId,
                    ServerStatus.SERVER_SESSION_STATE_CHANGED);
                MysqlResponseHandler.responseOk(okPacket, mysqlConnection.channel);
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
                commands.executeShowFields(table, packetId, mysqlConnection);
                break;
            case NativeConstants.COM_CREATE_DB:
                // create database
                break;
            case NativeConstants.COM_DROP_DB:
                // drop database
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
                okPacket = MysqlPacketFactory.getInstance().getOkPacket(0, packetId, 0);
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
                int paramCount = 0;
                boolean isSelect = false;
                DingoPreparedStatement preparedStatement;
                try {
                    preparedStatement
                        = (DingoPreparedStatement) connection.getStatement(new Meta.StatementHandle(connection.id,
                        statementId, null));
                    isSelect = preparedStatement.getStatementType() == Meta.StatementType.SELECT;
                    paramCount = preparedStatement.getParameterCount();
                } catch (NoSuchStatementException e) {
                    throw new RuntimeException(e);
                }
                ExecuteStatementPacket statementPacket = new ExecuteStatementPacket(paramCount);
                statementPacket.read(array);
                commands.executeStatement(statementPacket, preparedStatement, isSelect, packetId, mysqlConnection);
                break;
            case NativeConstants.COM_STMT_SEND_LONG_DATA:
                // send blob data
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
                    MysqlResponseHandler.responseError(packetId, mysqlConnection.channel, e);
                }
                break;
            case NativeConstants.COM_STMT_RESET:
                // destroy prepare sql param cache  : statement reset
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
