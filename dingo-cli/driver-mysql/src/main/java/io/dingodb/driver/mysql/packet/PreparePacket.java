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

import io.netty.buffer.ByteBuf;
import lombok.Builder;

import java.util.List;

@Builder
public class PreparePacket extends MysqlPacket {
    public PrepareOkPacket prepareOkPacket;

    List<ColumnPacket> paramColumnPackets;

    List<ColumnPacket> fieldsColumnPackets;

    @Override
    public int calcPacketSize() {
        return 0;
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
        prepareOkPacket.write(buffer);
        if (paramColumnPackets != null) {
            paramColumnPackets.forEach(columnPacket -> {
                columnPacket.write(buffer);
            });
        }
        if (fieldsColumnPackets != null) {
            fieldsColumnPackets.forEach(columnPacket -> {
                columnPacket.write(buffer);
            });
        }
    }
}
