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

package io.dingodb.driver.mysql.util;

import io.dingodb.common.mysql.MysqlByteUtil;
import io.netty.buffer.ByteBuf;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class BufferUtil {
    public static final BigInteger NEGATIVE_INC_VAL = new BigInteger("2").pow(64);

    public static void writeUB2(ByteBuf buffer, int operand) {
        buffer.writeByte(operand & 0xff);
        buffer.writeByte((byte) (operand >>> 8));
    }

    public static void writeUB3(ByteBuf buffer, int operand) {
        buffer.writeByte((byte) (operand & 0xff));
        buffer.writeByte((byte) (operand >>> 8));
        buffer.writeByte((byte) (operand >>> 16));
    }

    public static void writeInt(ByteBuf buffer, int operand) {
        buffer.writeByte((byte) (operand & 0xff));
        buffer.writeByte((byte) (operand >>> 8));
        buffer.writeByte((byte) (operand >>> 16));
        buffer.writeByte((byte) (operand >>> 24));
    }

    public static void writeFloat(ByteBuf buffer, float operand) {
        writeInt(buffer, Float.floatToIntBits(operand));
    }

    public static void writeUB4(ByteBuf buffer, long operand) {
        buffer.writeByte((byte) (operand & 0xff));
        buffer.writeByte((byte) (operand >>> 8));
        buffer.writeByte((byte) (operand >>> 16));
        buffer.writeByte((byte) (operand >>> 24));
    }

    public static void writeLong(ByteBuf buffer, long operand) {
        buffer.writeByte((byte) (operand & 0xff));
        buffer.writeByte((byte) (operand >>> 8));
        buffer.writeByte((byte) (operand >>> 16));
        buffer.writeByte((byte) (operand >>> 24));
        buffer.writeByte((byte) (operand >>> 32));
        buffer.writeByte((byte) (operand >>> 40));
        buffer.writeByte((byte) (operand >>> 48));
        buffer.writeByte((byte) (operand >>> 56));
    }

    public static void writeDouble(ByteBuf buffer, double operand) {
        writeLong(buffer, Double.doubleToLongBits(operand));
    }

    public static void writeLength(ByteBuf buffer, long operand) {
        if (operand < 0) {
            buffer.writeByte((byte)254);
            BigInteger operandTmp = new BigInteger(String.valueOf(operand));
            BigInteger operandFinal = NEGATIVE_INC_VAL.add(operandTmp);
            byte[] original = operandFinal.toByteArray();
            byte[] actual = new byte[8];
            System.arraycopy(original, 1, actual, 0, actual.length);
            for (int i = 7; i >= 0; i--) {
                buffer.writeByte(actual[i]);
            }
        } else if (operand < 251) {
            buffer.writeByte((byte) operand);
        } else if (operand < 0x10000L) {
            buffer.writeByte((byte) 252);
            writeUB2(buffer, (int) operand);
        } else if (operand < 0x1000000L) {
            buffer.writeByte((byte) 253);
            writeUB3(buffer, (int) operand);
        } else {
            buffer.writeByte((byte) 254);
            writeLong(buffer, operand);
        }
    }

    public static void writeWithNull(ByteBuf buffer, byte[] src) {
        buffer.writeBytes(src);
        buffer.writeByte((byte) 0);
    }

    public static void writeWithLength(ByteBuf buffer, byte[] src) {
        int length = src.length;
        if (length < 251) {
            buffer.writeByte((byte) length);
        } else if (length < 0x10000L) {
            buffer.writeByte((byte) 252);
            writeUB2(buffer, length);
        } else if (length < 0x1000000L) {
            buffer.writeByte((byte) 253);
            writeUB3(buffer, length);
        } else {
            buffer.writeByte((byte) 254);
            writeLong(buffer, length);
        }
        buffer.writeBytes(src);
    }

    public static int getLength(long length) {
        if (length < 0) {
            return 9;
        } else if (length < 251) {
            return 1;
        } else if (length < 0x10000L) {
            return 3;
        } else if (length < 0x1000000L) {
            return 4;
        } else {
            return 9;
        }
    }

    public static int getLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

    public static String getBinaryStrFromByte(byte byteVar) {
        String result = "";
        byte a = byteVar;
        ;
        for (int i = 0; i < 8; i++) {
            byte c = a;
            a = (byte) (a >> 1);
            a = (byte) (a << 1);
            if (a == c) {
                result = "0" + result;
            } else {
                result = "1" + result;
            }
            a = (byte) (a >> 1);
        }
        return result;
    }

    public static void writeDate(ByteBuf byteBuf, Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(date.getTime());
        short year = (short) calendar.get(Calendar.YEAR);
        byte month = (byte) (calendar.get(Calendar.MONTH) + 1);
        byte day = (byte) calendar.get(Calendar.DAY_OF_MONTH);
        byteBuf.writeBytes(MysqlByteUtil.shortToBytesLittleEndian(year));
        byteBuf.writeByte(month);
        byteBuf.writeByte(day);
    }

    public static void writeDateTime(ByteBuf byteBuf, Timestamp timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp.getTime());
        short year = (short) calendar.get(Calendar.YEAR);
        byte month = (byte) (calendar.get(Calendar.MONTH) + 1);
        byte day = (byte) calendar.get(Calendar.DAY_OF_MONTH);
        byte hour = (byte) calendar.get(Calendar.HOUR_OF_DAY);
        byte minute = (byte) calendar.get(Calendar.MINUTE);
        byte second = (byte) calendar.get(Calendar.SECOND);
        //int millis = calendar.get(Calendar.MILLISECOND);
        byteBuf.writeBytes(MysqlByteUtil.shortToBytesLittleEndian(year));
        byteBuf.writeByte(month);
        byteBuf.writeByte(day);
        byteBuf.writeByte(hour);
        byteBuf.writeByte(minute);
        byteBuf.writeByte(second);
        byteBuf.writeInt(0);
    }

    public static void writeTime(ByteBuf byteBuf, Time time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time.getTime());
        byte hour = (byte) calendar.get(Calendar.HOUR_OF_DAY);
        byte minute = (byte) calendar.get(Calendar.MINUTE);
        byte second = (byte) calendar.get(Calendar.SECOND);
        int millis = calendar.get(Calendar.MILLISECOND);
        // positive
        byteBuf.writeByte(0x00);
        // days
        byteBuf.writeInt(0);
        // hour
        byteBuf.writeByte(hour);
        byteBuf.writeByte(minute);
        byteBuf.writeByte(second);
        byteBuf.writeInt(millis);
    }

}
