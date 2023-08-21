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

public class AuthPacket extends MysqlPacket {
    private static final byte[] FILLER = new byte[23];

    public int clientFlags;

    public int extendClientFlags;
    public long maxPacketSize;
    public int charsetIndex;
    public byte[] extra;
    public String user;
    public byte[] password;
    public String database;
    public String clientAuthPlugin;

    public boolean isSSL;

    public boolean interActive;

    @Override
    public void read(byte[] data) {
        MysqlMessage mm = new MysqlMessage(data);
        clientFlags = mm.readUB2();
        if ((clientFlags & CapabilityFlags.CLIENT_INTERACTIVE.getCode()) != 0) {
            interActive = true;
        }
        extendClientFlags = mm.readUB2();
        maxPacketSize = mm.readUB4();
        charsetIndex = (mm.read() & 0xff);
        int current = mm.position();
        int len = (int) mm.readLength();
        if (len > 0 && len < FILLER.length) {
            byte[] ab = new byte[len];
            System.arraycopy(mm.bytes(), mm.position(), ab, 0, len);
            this.extra = ab;
        }
        mm.position(current + FILLER.length);
        user = mm.readStringWithNull();
        if (mm.position() + 1 >= mm.length()) {
            isSSL = true;
        } else {
            password = mm.readBytesWithLength();
            if (((clientFlags & CapabilityFlags.CLIENT_CONNECT_WITH_DB.getCode()) != 0)
                && mm.hasRemaining()) {
                database = mm.readStringWithNull();
            }
            clientAuthPlugin = mm.readStringWithNull();
        }
    }

    @Override
    public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        BufferUtil.writeUB4(buffer, clientFlags);
        BufferUtil.writeUB4(buffer, maxPacketSize);
        buffer.writeByte((byte) 8);
        buffer.writeBytes(FILLER);
        if (user == null) {
            buffer.writeByte((byte) 0);
        } else {
            BufferUtil.writeWithNull(buffer, user.getBytes());
        }
        if (password == null) {
            buffer.writeByte((byte) 0);
        } else {
            BufferUtil.writeWithLength(buffer, password);
        }
        if (database == null) {
            buffer.writeByte((byte) 0);
        } else {
            BufferUtil.writeWithNull(buffer, database.getBytes());
        }
    }

    @Override
    public int calcPacketSize() {
        int size = 32;// 4+4+1+23;
        size += (user == null) ? 1 : user.length() + 1;
        size += (password == null) ? 1 : BufferUtil.getLength(password);
        size += (database == null) ? 1 : database.length() + 1;
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Authentication Packet";
    }

}
