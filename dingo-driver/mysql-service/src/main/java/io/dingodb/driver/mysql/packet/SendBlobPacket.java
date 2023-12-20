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
import lombok.Getter;

public class SendBlobPacket extends MysqlPacket {

    public byte flag;

    @Getter
    private int statementId;
    @Getter
    private int parameter;
    @Getter
    private byte[] payload;

    @Override
    public int calcPacketSize() {
        // init flag = 1
        int size = 1;
        size += payload.length;
        // parameter size
        size += 2;
        // statementId size
        size += 4;
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL blob Packet";
    }

    @Override
    public void read(byte[] data) {
        MysqlMessage mm = new MysqlMessage(data);
        packetId = mm.read();
        flag = mm.read();
        statementId = mm.readInt();
        parameter = mm.readUB2();
        payload = mm.readBytes();
    }

    @Override
    public void write(ByteBuf buffer) {
        int size = calcPacketSize();
        BufferUtil.writeUB3(buffer, size);
        buffer.writeByte(packetId);
        buffer.writeByte(NativeConstants.COM_STMT_SEND_LONG_DATA);
        buffer.writeInt(statementId);
        buffer.writeShort(parameter);
        buffer.writeBytes(payload);
    }
}
