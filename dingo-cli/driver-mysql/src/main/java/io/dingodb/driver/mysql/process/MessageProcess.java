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

import io.dingodb.common.mysql.constant.ServerStatus;
import io.dingodb.driver.DingoConnection;
import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.driver.mysql.command.DingoCommands;
import io.dingodb.driver.mysql.command.MysqlResponseHandler;
import io.dingodb.driver.mysql.packet.MysqlPacket;
import io.dingodb.driver.mysql.packet.OKPacket;
import io.dingodb.driver.mysql.packet.QueryPacket;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MessageProcess {

    public static final DingoCommands commands = new DingoCommands();

    public static List<MysqlPacket> process(ByteBuf msg, MysqlConnection mysqlConnection) {
        int length = msg.readableBytes();
        byte[] array = new byte[length];
        msg.getBytes(msg.readerIndex(), array);
        byte flg = array[1];
        List<MysqlPacket> mysqlPacketList = null;
        byte packetIdByte = array[0];
        AtomicLong packetId = new AtomicLong(packetIdByte);
        packetId.incrementAndGet();
        switch (flg) {
            case 0x01:
                // quit
                if (mysqlConnection.channel.isActive()) {
                    mysqlConnection.channel.close();
                    return null;
                }
                break;
            case 0x02:
                // init db
                // use database
                byte[] schemaBytes = new byte[length - 2];
                System.arraycopy(array, 2, schemaBytes, 0, schemaBytes.length);
                DingoConnection connection = (DingoConnection) mysqlConnection.connection;
                String usedSchema = new String(schemaBytes);
                usedSchema = usedSchema.toUpperCase();
                CalciteSchema schema = connection.getContext().getRootSchema().getSubSchema(usedSchema, true);
                connection.getContext().setUsedSchema(schema);
                OKPacket okPacket = MysqlResponseHandler.getOkPacket(0, packetId,
                    ServerStatus.SERVER_SESSION_STATE_CHANGED);
                MysqlResponseHandler.responseOk(okPacket, mysqlConnection.channel);
                break;
            case 0x03:
                QueryPacket queryPacket = new QueryPacket();
                queryPacket.read(array);
                queryPacket.extendClientFlg = mysqlConnection.authPacket.extendClientFlags;
                queryPacket.clientFlg = mysqlConnection.authPacket.clientFlags;

                commands.execute(queryPacket, mysqlConnection);
                break;
            case 0x04:
                // show fields by tableName
                byte[] tableBytes = new byte[length - 2];
                System.arraycopy(array, 2, tableBytes, 0, tableBytes.length);
                String table = new String(tableBytes);
                commands.executeShowFields(table, packetId, mysqlConnection);
                break;
            case 0x05:
                // create database
                break;
            case 0x06:
                // drop database
                break;
            case 0x07:
                // clean cache
            case 0x08:
                // stop server
                break;
            case 0x09:
                // get server statistical information
                break;
            case 0x0A:
                // get current connection list
                break;
            case 0x0B:
                // inner thread status
                break;
            case 0x0C:
                // break n connection
                break;
            case 0x0D:
                // save server debug information
                break;
            case 0x0E:
                // test ping
                break;
            case 0x0F:
            case 0x10:
            case 0x11:
            case 0x12:
            case 0x13:
            case 0x14:
            case 0x15:
            case 0x16:
                // prepare sql
                break;
            case 0x17:
                // execute prepare sql
                break;
            case 0x18:
                // send blob data
                break;
            case 0x19:
                // destroy prepare sql
                break;
            case 0x1A:
                // destroy prepare sql param cache
                break;
            case 0x1B:
                // set option
                break;
            case 0x1C:
                // fetch prepare statement result
                break;
            default:
                break;
        }
        return mysqlPacketList;
    }
}
