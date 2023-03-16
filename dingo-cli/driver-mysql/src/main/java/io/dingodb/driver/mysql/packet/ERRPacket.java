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

import io.dingodb.common.mysql.CapabilityFlags;
import io.dingodb.common.mysql.MysqlMessage;
import io.dingodb.driver.mysql.util.BufferUtil;
import io.netty.buffer.ByteBuf;

public class ERRPacket extends MysqlPacket {

    public int header;

    public int errorCode;

    public byte sqlStateMarker;

    public String sqlState;

    public String errorMessage;

    public int capabilities;

    @Override
    public void read(byte[] data) {
        MysqlMessage message = new MysqlMessage(data);
        //this.packetLength = message.readUB3();
        this.packetId = message.read();
        this.header = message.read();
        this.errorCode = message.readUB2();
        if ((capabilities & CapabilityFlags.CLIENT_PROTOCOL_41.getCode()) > 0) {
            sqlStateMarker = message.read();
            sqlState = new String(message.readBytes(5));
        }
        errorMessage = message.readString();
    }

    @Override
    public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte((byte) 0xff);
        BufferUtil.writeUB2(buffer, errorCode);
        if ((capabilities & CapabilityFlags.CLIENT_PROTOCOL_41.getCode()) > 0) {
            buffer.writeByte((byte) '#');
            buffer.writeBytes(sqlState.getBytes());
        }
        if (errorMessage != null) {
            buffer.writeBytes(errorMessage.getBytes());
        }
    }

    @Override
    public int calcPacketSize() {
        int size = 9;
        if (errorMessage != null) {
            size += errorMessage.length();
        }
        return size;
    }

    @Override
    public String getPacketInfo() {
        return "MySQL Error Packet";
    }

    @Override
    public String toString() {
        return "ERRPacket{"
                + "packetLength=" + packetLength
                + ", packetSequenceId=" + packetId
                + ", header=" + header
                + ", errorCode=" + errorCode
                + ", sqlStateMarker=" + sqlStateMarker
                + ", sqlState=" + sqlState
                + ", errorMessage='" + errorMessage + '\''
                + ", capabilities=" + capabilities
                + "}\n";
    }
}
