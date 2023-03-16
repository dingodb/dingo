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
import io.dingodb.driver.mysql.util.BufferUtil;
import io.netty.buffer.ByteBuf;

public class ColumnsNumberPacket extends MysqlPacket {

    public long columnsNumber;

    @Override
    public void read(byte[] data) {
        MysqlMessage message = new MysqlMessage(data);
        //packetLength = message.readUB3();
        packetId = message.read();
        columnsNumber = message.readLength();
    }

    @Override
    public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte((byte) columnsNumber);
    }

    @Override
    public int calcPacketSize() {
        return 1;
    }

    @Override
    public String getPacketInfo() {
        return "response to a COM_QUERY packet";
    }

    @Override
    public String toString() {
        return "ColumnsNumberPacket{"
                + "  columnsNumber=" + columnsNumber
                + ", packetLength=" + packetLength
                + ", packetSequenceId=" + packetId
                + "}\n";
    }
}
