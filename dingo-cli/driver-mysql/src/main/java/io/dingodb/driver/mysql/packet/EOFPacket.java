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
import io.dingodb.common.mysql.MysqlServer;
import io.dingodb.driver.mysql.util.BufferUtil;
import io.netty.buffer.ByteBuf;

public class EOFPacket extends MysqlPacket {

    public byte header;

    public short warningCount;

    public int statusFlags;

    public int capabilities = MysqlServer.getServerCapabilities();

    @Override
    public void read(byte[] data) {
        MysqlMessage message = new MysqlMessage(data);
        //packetLength = message.readUB3();
        packetId = message.read();
        header = message.read();
        if ((capabilities & CapabilityFlags.CLIENT_PROTOCOL_41.getCode()) != 0) {
            warningCount = (short) message.readUB2();
            statusFlags = (short) message.readUB2();
        } else {
            message.move(4);
        }
    }

    @Override
    public void write(ByteBuf buffer) {
        int size = calcPacketSize();
        BufferUtil.writeUB3(buffer, size);
        buffer.writeByte(packetId);
        buffer.writeByte((byte) header);
        if ((capabilities & CapabilityFlags.CLIENT_PROTOCOL_41.getCode()) != 0) {
            BufferUtil.writeUB2(buffer, warningCount);
            BufferUtil.writeUB2(buffer, statusFlags);
        }
    }

    @Override
    public int calcPacketSize() {
        return 5;
    }

    @Override
    public String getPacketInfo() {
        return "MySQL EOF Packet";
    }

    @Override
    public String toString() {
        return "EOFPacket{"
                + "  packetLength=" + packetLength
                + ", packetSequenceId=" + packetId
                + ", header=" + header
                + ", warnings=" + warningCount
                + ", statusFlags=" + statusFlags
                + ", capabilities=" + capabilities
                + "}\n";
    }
}
