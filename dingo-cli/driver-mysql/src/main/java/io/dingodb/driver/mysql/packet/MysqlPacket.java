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

import io.netty.buffer.ByteBuf;

public abstract class MysqlPacket {

    public static final byte COM_SLEEP = 0;

    public static final byte COM_QUIT = 1;

    public static final byte COM_INIT_DB = 2;

    public static final byte COM_QUERY = 3;

    public static final byte COM_FIELD_LIST = 4;

    public static final byte COM_CREATE_DB = 5;

    public static final byte COM_DROP_DB = 6;

    public static final byte COM_REFRESH = 7;

    public static final byte COM_SHUTDOWN = 8;

    public static final byte COM_STATISTICS = 9;

    public static final byte COM_PROCESS_INFO = 10;

    public static final byte COM_CONNECT = 11;

    public static final byte COM_PROCESS_KILL = 12;

    public static final byte COM_DEBUG = 13;

    public static final byte COM_PING = 14;

    public static final byte COM_TIME = 15;

    public static final byte COM_DELAYED_INSERT = 16;

    public static final byte COM_CHANGE_USER = 17;

    public static final byte COM_BINLOG_DUMP = 18;

    public static final byte COM_TABLE_DUMP = 19;

    public static final byte COM_CONNECT_OUT = 20;

    public static final byte COM_REGISTER_SLAVE = 21;

    public static final byte COM_STMT_PREPARE = 22;

    public static final byte COM_STMT_EXECUTE = 23;

    public static final byte COM_STMT_SEND_LONG_DATA = 24;

    public static final byte COM_STMT_CLOSE = 25;

    public static final byte COM_STMT_RESET = 26;

    public static final byte COM_SET_OPTION = 27;

    public static final byte COM_STMT_FETCH = 28;

    public int packetLength;

    public byte packetId;

    public abstract int calcPacketSize();

    protected abstract String getPacketInfo();

    public abstract void read(byte[] data);

    public abstract void write(ByteBuf buffer);

    @Override
    public String toString() {
        return new StringBuilder().append(getPacketInfo()).append("{length=")
            .append(packetLength).append(",id=").append(packetId)
            .append('}').toString();
    }

}
