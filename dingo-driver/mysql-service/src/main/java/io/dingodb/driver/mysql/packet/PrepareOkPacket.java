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

public class PrepareOkPacket extends OKPacket {

    public int statementId;

    public int numberFields;

    public int numberParams;

    @Override
    public int calcPacketSize() {
        int i = 1;
        i += 4;
        i += 2;
        i += 2;
        i += 1;
        i += 2;
        return i;
    }

    @Override
    protected String getPacketInfo() {
        return null;
    }

    @Override
    public void read(byte[] data) {

    }

    @Override
    public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(header);
        BufferUtil.writeUB4(buffer, statementId);
        BufferUtil.writeUB2(buffer, numberFields);
        BufferUtil.writeUB2(buffer, numberParams);
        buffer.writeByte(0);
        BufferUtil.writeUB2(buffer, warningCount);
    }
}
