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

import io.dingodb.driver.mysql.util.BufferUtil;
import io.netty.buffer.ByteBuf;

public class LoadDataResPacket extends MysqlPacket {
    public String message;

    @Override
    public int calcPacketSize() {
        int i = 1;

        if (message != null) {
            i += message.getBytes().length;
        }
        return i;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL OK Packet";
    }

    @Override
    public void read(byte[] data) {

    }

    @Override
    public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(0xfb);
        buffer.writeBytes(message.getBytes());
    }
}
