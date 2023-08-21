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


import io.dingodb.common.mysql.MysqlByteUtil;
import io.dingodb.common.mysql.MysqlMessage;
import io.dingodb.driver.mysql.util.BufferUtil;
import io.netty.buffer.ByteBuf;

public class HandshakePacket extends MysqlPacket {
    private static final byte[] FILLER_13 = new byte[]{0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0};

    public byte protocolVersion;
    public byte[] serverVersion;
    public int threadId;

    // 8 bytes
    public byte[] seed;
    public int serverCapabilities;
    public byte serverCharsetIndex;
    public short serverStatus;

    public short extendedServer;

    public byte authPluginLength;

    // 10 byte
    public byte[] unused;

    // 13 byte
    public byte[] seed2; //这个其实就是seed2

    public byte[] authPlugin;

    @Override
    public void read(byte[] data) {
        MysqlMessage mm = new MysqlMessage(data);
        //packetLength = mm.readUB3();
        packetId = mm.read();
        protocolVersion = mm.read();
        serverVersion = mm.readBytesWithNull();
        threadId = (int) mm.readUB4();
        seed = mm.readBytesWithNull();
        serverCapabilities = (short) mm.readUB2();
        serverCharsetIndex = mm.read();
        serverStatus = (short) mm.readUB2();
        mm.move(13);
        seed2 = mm.readBytesWithNull();
    }

    @Override
    public int calcPacketSize() {
        // protocol
        int size = 1;
        // serverVersion
        size += serverVersion.length;
        // 00
        size += 1;
        // thread id
        size += 4;
        // salt
        size += seed.length;
        // 0x00
        size += 1;
        // server capabilities
        size += 2;
        // server language
        size += 1;
        // server status
        size += 2;
        // extend server capabilities
        size += 2;
        // auth plugin length
        size += 1;
        // unused
        size += 10;
        // salt 2
        size += 13;
        // auth plugin
        size += authPlugin.length;
        // 00
        size += 1;
        return size;
    }

    @Override
    public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(protocolVersion);
        BufferUtil.writeWithNull(buffer, serverVersion);
        buffer.writeBytes(MysqlByteUtil.intToBytesLittleEndian(threadId));
        BufferUtil.writeWithNull(buffer, seed);
        BufferUtil.writeUB2(buffer, serverCapabilities);
        buffer.writeByte(serverCharsetIndex);
        BufferUtil.writeUB2(buffer, serverStatus);
        BufferUtil.writeUB2(buffer, extendedServer);
        buffer.writeByte(authPluginLength);
        buffer.writeBytes(unused);
        BufferUtil.writeWithNull(buffer, seed2);
        BufferUtil.writeWithNull(buffer, authPlugin);
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Handshake Packet";
    }

}
