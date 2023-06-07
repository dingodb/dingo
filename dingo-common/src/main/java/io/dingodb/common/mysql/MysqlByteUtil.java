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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
        byte[] yearBytes = new byte[2];
        System.arraycopy(bytes, 0, yearBytes, 0, yearBytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(yearBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN); // 设置为小端字节序
        int year = buffer.getShort();

        int month = bytes[2] & 0xff;
        int day = bytes[3] & 0xff;


        LocalDate localDate = LocalDate.now();
        localDate = localDate.withYear(year).withMonth(month).withDayOfMonth(day);
        return localDate.atStartOfDay(ZoneId.systemDefault())
            .toInstant()
            .toEpochMilli();
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
        byte[] yearBytes = new byte[2];
        System.arraycopy(bytes, 0, yearBytes, 0, yearBytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(yearBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN); // 设置为小端字节序
        int year = buffer.getShort();

        int month = bytes[2] & 0xff;
        int day = bytes[3] & 0xff;
        int hour =  bytes[4] & 0xff;
        int minute = bytes[5] & 0xff;
        int second = bytes[6] & 0xff;
        LocalDateTime dateTime = LocalDateTime.now();
        dateTime = dateTime.withYear(year)
            .withMonth(month)
            .withDayOfMonth(day)
            .withHour(hour)
            .withMinute(minute)
            .withSecond(second);
        return Timestamp.valueOf(dateTime);
    }

}
