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

package io.dingodb.common.mysql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class MysqlByteUtil {
    public static int bytesToIntLittleEndian(byte[] bytes) {
        // byte数组中序号小的在右边
        return bytes[0] & 0xFF | //
            (bytes[1] & 0xFF) << 8 | //
            (bytes[2] & 0xFF) << 16 | //
            (bytes[3] & 0xFF) << 24; //
    }

    public static int bytesToIntBigEndian(byte[] bytes) {
        // byte数组中序号大的在右边
        return bytes[3] & 0xFF | //
            (bytes[2] & 0xFF) << 8 | //
            (bytes[1] & 0xFF) << 16 | //
            (bytes[0] & 0xFF) << 24; //
    }

    public static byte[] intToBytesBigEndian(int operand) {
        byte[] b = new byte[4];
        b[3] = (byte) (operand & 0xff);
        b[2] = (byte) (operand >> 8 & 0xff);
        b[1] = (byte) (operand >> 16 & 0xff);
        b[0] = (byte) (operand >> 24 & 0xff);
        return b;
    }


    public static byte[] intToBytesLittleEndian(int operand) {
        byte[] b = new byte[4];
        b[0] = (byte) (operand & 0xff);
        b[1] = (byte) (operand >> 8 & 0xff);
        b[2] = (byte) (operand >> 16 & 0xff);
        b[3] = (byte) (operand >> 24 & 0xff);
        return b;
    }

    public static byte[] shortToBytesBigEndian(short operand) {
        byte[] b = new byte[2];
        b[1] = (byte) (operand & 0xff);
        b[0] = (byte) (operand >> 8 & 0xff);
        return b;
    }

    public static byte[] shortToBytesLittleEndian(short operand) {
        byte[] b = new byte[2];
        b[0] = (byte) (operand & 0xff);
        b[1] = (byte) (operand >> 8 & 0xff);
        return b;
    }

    public static short bytesToShortLittleEndian(byte[] bytes) {
        return (short) (((bytes[1] << 8) | bytes[0] & 0xff));
    }

    public static short bytesToShortBigEndian(byte[] bytes) {
        return (short) (((bytes[0] << 8) | bytes[1] & 0xff));
    }

    public static byte[] hexStringToByteArray(String hexStr) {
        hexStr = hexStr.toUpperCase();
        int len = hexStr.length() / 2;
        char[] hexChars = hexStr.toCharArray();
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            int pos = i * 2;
            bytes[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return bytes;
    }

    public static byte charToByte(char operand) {
        return (byte) "0123456789ABCDEF".indexOf(operand);
    }

    public static long bytesToLongLittleEndian(byte[] bytes) {
        int position = 0;
        long i = bytes[position++] & 0xff;
        i |= (long) (bytes[position++] & 0xff) << 8;
        i |= (long) (bytes[position++] & 0xff) << 16;
        i |= (long) (bytes[position++] & 0xff) << 24;
        i |= (long) (bytes[position++] & 0xff) << 32;
        i |= (long) (bytes[position++] & 0xff) << 40;
        i |= (long) (bytes[position++] & 0xff) << 48;
        i |= (long) (bytes[position++] & 0xff) << 56;
        return i;
    }

    public static long bytesToDateLittleEndian(byte[] bytes) {
        Calendar calendar = getCalendar(bytes);
        return calendar.getTimeInMillis();
    }

    public static Time bytesToTimeLittleEndian(byte[] bytes) {
        //byte flags = bytes[0];
        byte[] dayBytes = new byte[4];
        System.arraycopy(bytes, 1, dayBytes, 0, dayBytes.length);
        //int day = bytesToIntLittleEndian(dayBytes);

        int hour = bytes[5] & 0xff;
        int minute = bytes[6] & 0xff;
        int second = bytes[7] & 0xff;
        //byte[] nsBytes = new byte[4];
        //System.arraycopy(bytes, 8, nsBytes, 0, dayBytes.length);
        //int ns = bytesToIntLittleEndian(nsBytes);
        Time time = new Time(hour, minute, second);
        return time;
    }

    public static Timestamp bytesToTimeStampLittleEndian(byte[] bytes) {
        Calendar calendar = getCalendar(bytes);
        calendar.set(Calendar.HOUR, bytes[4] & 0xff);
        calendar.set(Calendar.MINUTE, bytes[5] & 0xff);
        calendar.set(Calendar.SECOND, bytes[6] & 0xff);
        Timestamp timestamp = new Timestamp(calendar.getTimeInMillis());
        return timestamp;
    }

    private static Calendar getCalendar(byte[] bytes) {
        byte[] yearBytes = new byte[2];
        System.arraycopy(bytes, 0, yearBytes, 0, yearBytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(yearBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN); // 设置为小端字节序
        int year = buffer.getShort();

        int month = bytes[2] & 0xff;
        int day = bytes[3] & 0xff;
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month - 1);
        calendar.set(Calendar.DAY_OF_MONTH, day);
        return calendar;
    }

}
