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

    public static byte[] intToByteBig(int operand) {
        byte[] b = new byte[4];
        b[3] = (byte) (operand & 0xff);
        b[2] = (byte) (operand >> 8 & 0xff);
        b[1] = (byte) (operand >> 16 & 0xff);
        b[0] = (byte) (operand >> 24 & 0xff);
        return b;
    }


    public static byte[] intToByteLittle(int operand) {
        byte[] b = new byte[4];
        b[0] = (byte) (operand & 0xff);
        b[1] = (byte) (operand >> 8 & 0xff);
        b[2] = (byte) (operand >> 16 & 0xff);
        b[3] = (byte) (operand >> 24 & 0xff);
        return b;
    }

    public static byte[] shortToByteBig(short operand) {
        byte[] b = new byte[2];
        b[1] = (byte) (operand & 0xff);
        b[0] = (byte) (operand >> 8 & 0xff);
        return b;
    }

    public static byte[] shortToByteLittle(short operand) {
        byte[] b = new byte[2];
        b[0] = (byte) (operand & 0xff);
        b[1] = (byte) (operand >> 8 & 0xff);
        return b;
    }

    public static short byteToShortLittle(byte[] operand) {
        return (short) (((operand[1] << 8) | operand[0] & 0xff));
    }

    public static short byteToShortBig(byte[] operand) {
        return (short) (((operand[0] << 8) | operand[1] & 0xff));
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


}
