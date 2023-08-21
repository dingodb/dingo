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

import io.dingodb.common.mysql.MysqlMessage;
import io.dingodb.driver.mysql.NativeConstants;
import io.dingodb.driver.mysql.util.BufferUtil;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class QueryPacket extends MysqlPacket {
    public byte flag;
    public byte[] message;
    public int extendClientFlg;

    public int clientFlg;

    public void read(byte[] data) {
        MysqlMessage mm = new MysqlMessage(data);
        //packetLength = mm.readUB3();
        packetId = mm.read();
        flag = mm.read();
        message = mm.readBytes();
    }

    public void write(ByteBuf buffer) {
        int size = calcPacketSize();
        BufferUtil.writeUB3(buffer, size);
        buffer.writeByte(packetId);
        buffer.writeByte(NativeConstants.COM_QUERY);
        buffer.writeBytes(message);
    }

    @Override
    public int calcPacketSize() {
        int size = 1;
        if (message != null) {
            size += message.length;
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Query Packet";
    }

}
