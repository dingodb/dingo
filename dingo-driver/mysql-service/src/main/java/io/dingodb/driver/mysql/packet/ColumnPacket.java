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
import lombok.Builder;

@Builder
public class ColumnPacket extends MysqlPacket {
    public String catalog;
    public String schema;

    public String table;

    public String orgTable;

    public String name;

    public String orgName;

    public short characterSet;

    public int columnLength;

    public byte type;

    public short flags;

    public byte decimals;

    @Override
    public void read(byte[] data) {
        MysqlMessage message = new MysqlMessage(data);
        //packetLength = message.readUB3();
        packetId = message.read();
        catalog = message.readStringWithLength();
        schema = message.readStringWithLength();
        table = message.readStringWithLength();
        orgTable = message.readStringWithLength();
        name = message.readStringWithLength();
        orgName = message.readStringWithLength();
        message.move(1);
        characterSet = (short) message.readUB2();
        columnLength = message.readInt();
        type = message.read();
        flags = (short) message.readUB2();
        decimals = message.read();
        message.move(2);
    }

    @Override
    public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        BufferUtil.writeWithLength(buffer, catalog.getBytes());
        BufferUtil.writeWithLength(buffer, schema.getBytes());
        BufferUtil.writeWithLength(buffer, table.getBytes());
        BufferUtil.writeWithLength(buffer, orgTable.getBytes());
        BufferUtil.writeWithLength(buffer, name.getBytes());
        BufferUtil.writeWithLength(buffer, orgName.getBytes());;
        buffer.writeByte((byte) 0x0c);
        byte[] charsetNum = MysqlByteUtil.shortToBytesLittleEndian(characterSet);
        buffer.writeBytes(charsetNum);
        byte[] length = MysqlByteUtil.intToBytesLittleEndian(columnLength);
        buffer.writeBytes(length);
        buffer.writeByte(type);
        buffer.writeBytes(MysqlByteUtil.shortToBytesLittleEndian(flags));
        buffer.writeByte(decimals);
        buffer.writeByte((byte) 0x00);
        buffer.writeByte((byte) 0x00);
    }

    @Override
    public int calcPacketSize() {
        int i = 0;
        i += BufferUtil.getLength(catalog.getBytes());
        i += BufferUtil.getLength(schema.getBytes());
        i += BufferUtil.getLength(table.getBytes());
        i += BufferUtil.getLength(orgTable.getBytes());
        i += BufferUtil.getLength(name.getBytes());
        i += BufferUtil.getLength(orgName.getBytes());
        // 0x0c
        i += 1;
        i += 2;
        i += 4;
        i += 1;
        i += 2;
        i += 1;
        // 0x00 0x00
        i += 2;
        return i;
    }

    @Override
    public String getPacketInfo() {
        return "MySQL Column Packet";
    }

    @Override
    public String toString() {
        return "ColumnPacket{"
                + "  packetLength=" + packetLength
                + ", packetSequenceId=" + packetId
                + ", catalog='" + catalog + '\''
                + ", schema='" + schema + '\''
                + ", table='" + table + '\''
                + ", orgTable='" + orgTable + '\''
                + ", name='" + name + '\''
                + ", orgName='" + orgName + '\''
                + ", characterSet=" + characterSet
                + ", columnLength=" + columnLength
                + ", type=" + type
                + ", flags=" + flags
                + ", decimals=" + decimals
                + "}\r\n";
    }
}
