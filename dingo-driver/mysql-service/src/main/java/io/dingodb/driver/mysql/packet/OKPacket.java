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

import java.math.BigInteger;

import static io.dingodb.driver.mysql.util.BufferUtil.NEGATIVE_INC_VAL;

public class OKPacket extends MysqlPacket {
    public static final byte HEADER = 0x00;

    public static final BigInteger zero = new BigInteger("0");
    public static final BigInteger level1 = new BigInteger("251");
    public static final BigInteger level2 = new BigInteger("65536");
    public static final BigInteger level3 = new BigInteger("16777216");
    public byte header = HEADER;
    public long affectedRows;
    public BigInteger insertId;
    public int serverStatus;
    public int warningCount;
    public byte[] message;

    public int capabilities;
    // there is no need to produce OKPacket every time,sometimes we can use OK
    // byte[] directly.
    public static final byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0,
        0};
    private static final byte[] AC_OFF = new byte[]{7, 0, 0, 1, 0, 0, 0, 0,
        0, 0, 0};

    @Override
    public void read(byte[] data) {
    }

    @Override
    public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(header);
        BufferUtil.writeLength(buffer, affectedRows);
        writeInsertId(buffer);
        BufferUtil.writeUB2(buffer, serverStatus);
        BufferUtil.writeUB2(buffer, warningCount);
        if (message != null) {
            BufferUtil.writeWithLength(buffer, message);
        }
    }

    @Override
    public int calcPacketSize() {
        int i = 1;
        i += BufferUtil.getLength(affectedRows);
        i += getInsertIdLength();
        // server status
        i += 2;
        // warnings
        i += 2;
        if (message != null) {
            i += BufferUtil.getLength(message);
        }
        return i;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL OK Packet";
    }

    private int getInsertIdLength() {
        if (insertId.compareTo(zero) < 0) {
            // if insertId is negative, transform to 2 to the power of 64 and occupying 8 bytes
            // format fe xx xx xx xx xx xx xx xx
            // 8 + 1 = 9
            return 9;
        } else if (insertId.compareTo(level1) < 0) {
            return 1;
        } else if (insertId.compareTo(level2) < 0) {
            // format fc xx xx
            return 3;
        } else if (insertId.compareTo(level3) < 0) {
            // format fd xx xx xx
            return 4;
        } else {
            // if insertId > 1677716, format remains the same as negative numbers
            return 9;
        }
    }

    public void writeInsertId(ByteBuf buffer) {
        if (insertId.compareTo(zero) < 0) {
            // if insertId is negative, transform to 2 to the power of 64 and occupying 8 bytes
            // format fe xx xx xx xx xx xx xx xx
            // 8 + 1 = 9
            processLargeNumberOrNegative(buffer);
        } else if (insertId.compareTo(level1) < 0) {
            buffer.writeByte(insertId.intValue());
        } else if (insertId.compareTo(level2) < 0) {
            // format fc xx xx
            // limit is 65536
            buffer.writeByte((byte)252);
            BufferUtil.writeUB2(buffer, insertId.intValue());
        } else if (insertId.compareTo(level3) < 0) {
            // format fd xx xx xx
            // limit is 16777216
            buffer.writeByte((byte)253);
            BufferUtil.writeUB3(buffer, insertId.intValue());
        } else {
            // if insertId > 1677716, format remains the same as negative numbers
            processLargeNumberOrNegative(buffer);
        }
    }

    private void processLargeNumberOrNegative(ByteBuf buffer) {
        buffer.writeByte(254);
        BigInteger operandFinal = NEGATIVE_INC_VAL.add(insertId);
        byte[] original = operandFinal.toByteArray();
        byte[] actual = new byte[8];
        System.arraycopy(original, 1, actual, 0, actual.length);
        for (int i = 7; i >= 0; i--) {
            buffer.writeByte(actual[i]);
        }
    }

}
