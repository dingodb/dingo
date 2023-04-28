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
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ResultSetRowPacket extends MysqlPacket {

    public List<String> values = new ArrayList<>();
    private static final byte NULL_MARK = (byte) 251;

    public long columnCount;

    @Override
    public void read(byte[] data) {
        MysqlMessage message = new MysqlMessage(data);
        //packetLength = message.readUB3();
        packetId = message.read();
        for (int i = 0; i < columnCount; i++) {
            values.add(message.readStringWithLength());
        }
    }

    @Override
    public void write(ByteBuf buffer) {
        int max = 16777215;
        int size = calcPacketSize();
        if (size > max) {
            ByteBuf buf = Unpooled.buffer(size, size);

            for (int i = 0; i < values.size(); i++) {
                byte[] fv = values.get(i).getBytes();
                if (fv == null) {
                    buf.writeByte(NULL_MARK);
                } else {
                    BufferUtil.writeLength(buf, fv.length);
                    buf.writeBytes(fv);
                }
            }

            int mod = 0;
            int loop = mod = size % max == 0 ? size / max : size / max + 1;
            while (loop > 0) {
                if (loop == 1) {
                    BufferUtil.writeUB3(buffer, mod);
                    buffer.writeByte(packetId);
                    buffer.writeBytes(buf.readBytes(mod));
                } else {
                    BufferUtil.writeUB3(buffer, max);
                    buffer.writeByte(packetId);
                    buffer.writeBytes(buf.readBytes(max));
                }
                packetId += 1;
                loop --;
            }

        } else {
            BufferUtil.writeUB3(buffer, calcPacketSize());
            buffer.writeByte(packetId);
            for (int i = 0; i < values.size(); i++) {
                byte[] fv = values.get(i).getBytes();
                if (fv == null) {
                    buffer.writeByte(NULL_MARK);
                } else {
                    BufferUtil.writeLength(buffer, fv.length);
                    buffer.writeBytes(fv);
                }
            }
        }
    }


    @Override
    public int calcPacketSize() {
        int size = 0;
        for (int i = 0; i < values.size(); i++) {
            byte[] v = values.get(i).getBytes();
            size += (v == null || v.length == 0) ? 1 : BufferUtil.getLength(v);
        }
        return size;
    }

    @Override
    public String getPacketInfo() {
        return "MySQL ResultSet Row Packet";
    }

    @Override
    public String toString() {
        return "ResultSetRowPacket{"
                + " packetLength=" + packetLength
                + ", packetSequenceId=" + packetId
                + ", values=" + values
                + "}\n";
    }

    public void addColumnValue(String val) {
        if (StringUtils.isBlank(val)) {
            val = "";
        }
        values.add(val);
    }
}
