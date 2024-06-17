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

package io.dingodb.common.codec;

import io.dingodb.common.mysql.MysqlByteUtil;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Pair;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Arrays;

@Slf4j
public final class CodecKvUtil {
    private static final int encGroupSize = 8;
    private static final int encGroupAllSize = 9;
    private static final int encMarker = 255;
    private static final byte[] prefix = "m".getBytes();
    private static final int prefixLen = prefix.length;
    private static final Long hashData = 104L;
    private static final byte endPad = 0x0;

    private CodecKvUtil() {
    }

    public static int encodeBytesLength(int dataLen) {
        int mod = dataLen % encGroupSize;
        int padCount = encGroupSize - mod;
        return dataLen + padCount + 1 + dataLen / encGroupSize;
    }

    public static byte[] encodeHashDataKey(byte[] key, byte[] data) {
        int prefixLen = prefix.length;
        int len = prefixLen + encodeBytesLength(key.length) + 8 + encodeBytesLength(data.length);
        byte[] ek = new byte[len];
        System.arraycopy(prefix, 0, ek, 0, prefixLen);
        byte[] keyBytes = encodeBytes(key);
        int keyBytesLen = keyBytes.length;
        System.arraycopy(keyBytes, 0, ek, prefixLen, keyBytesLen);

        byte[] hashDataBytes = MysqlByteUtil.toByteArray(hashData);
        int hashDataBytesLen = hashDataBytes.length;
        System.arraycopy(hashDataBytes, 0, ek,prefixLen + keyBytesLen, hashDataBytes.length);
        byte[] dataBytes = encodeBytes(data);
        int dataBytesLen = dataBytes.length;
        System.arraycopy(dataBytes, 0, ek, prefixLen + keyBytesLen + hashDataBytesLen, dataBytesLen);
        return ek;
    }

    public static Pair<byte[], byte[]> decodeHashDataKey(byte[] ek) {
        if (!hasPrefix(ek, prefix)) {
           log.error("invalid encoded hash data key prefix");
           throw new RuntimeException("invalid encoded hash data key prefix");
        }
        int prefixLen = prefix.length;
        int ekLen = ek.length;
        byte[] tmp = new byte[ek.length - prefixLen];

        System.arraycopy(ek, prefixLen, tmp, 0, ekLen - prefixLen);
        ek = tmp;

        Pair<byte[], byte[]> pair = decodeBytes(ek);

        ek = pair.getKey();
        byte[] key = pair.getValue();

        byte[] hashDataKey = new byte[8];
        System.arraycopy(ek, 0, hashDataKey, 0, hashDataKey.length);
        ByteBuffer buffer = ByteBuffer.wrap(hashDataKey);
        long value = buffer.getLong();
        if (value != 104) {
            throw new RuntimeException("invalid encoded hash data key");
        }
        tmp = new byte[ek.length - 8];
        System.arraycopy(ek, 8, tmp, 0, tmp.length);
        ek = tmp;

        pair = decodeBytes(ek);
        byte[] field = pair.getValue();
        return Pair.of(key, field);
    }

    public static Pair<byte[], byte[]> decodeBytes(byte[] base) {
        return decodeBytes(base, false);
    }

    public static Pair<byte[], byte[]> decodeBytes(byte[] base, boolean reverse) {
        byte[] buf = new byte[0];
        while (true) {
            int baseLen = base.length;
            if (baseLen < encGroupAllSize) {
                throw new RuntimeException("insufficient bytes to decode value");
            }
            byte[] groupBytes = new byte[encGroupAllSize];
            System.arraycopy(base, 0, groupBytes, 0, groupBytes.length);
            byte[] group = new byte[encGroupSize];
            System.arraycopy(groupBytes, 0, group, 0, encGroupSize);
            byte marker = groupBytes[encGroupSize];

            byte padCount;
            if (reverse) {
                padCount = marker;
            } else {
                padCount = (byte) (encMarker - marker);
            }
            if (padCount > encGroupSize) {
                throw new RuntimeException("invalid marker byte");
            }

            int realGroupSize = encGroupSize - padCount;

            if (!isEmpty(group, marker)) {
                byte[] tmp = new byte[buf.length + realGroupSize];
                System.arraycopy(buf, 0, tmp, 0, buf.length);
                System.arraycopy(group, 0, tmp, buf.length, realGroupSize);
                buf = tmp;
            }

            byte[] baseTmp = new byte[baseLen - encGroupAllSize];
            System.arraycopy(base, encGroupAllSize, baseTmp, 0, baseTmp.length);
            base = baseTmp;
            if (padCount != 0) {
                byte padByte = endPad;
                if (reverse) {
                    padByte = (byte) encMarker;
                }
                for (int i = realGroupSize; i < group.length; i ++) {
                    byte v = group[i];
                    if (v != padByte) {
                        throw new RuntimeException("invalid padding byte");
                    }
                }
                break;
            }
        }
        if (reverse) {
            // to implement
        }

        return Pair.of(base, buf);
    }

    public static boolean isEmpty(byte[] group, byte marker) {
        if (marker == 0) {
            boolean notEquqZero = false;
            for (byte b : group) {
                if (b != 0) {
                    notEquqZero = true;
                    break;
                }
            }
            return !notEquqZero;
        }
        return false;
    }

    public static boolean hasPrefix(byte[] ek, byte[] prefix) {
        int compare = ByteArrayUtils.compare(ek, prefix, true);
        return ek.length >= prefix.length && compare == 0;
    }

    public static byte[] hashDataKeyPrefix(byte[] key) {
        byte[] keyBytes = encodeBytes(key);
        int keyBytesLen = keyBytes.length;

        byte[] hashDataBytes = MysqlByteUtil.toByteArray(hashData);
        int hashDataBytesLen = hashDataBytes.length;
        byte[] ek = new byte[prefixLen + keyBytesLen + hashDataBytesLen];

        System.arraycopy(prefix, 0, ek, 0, prefixLen);
        System.arraycopy(keyBytes, 0, ek, prefix.length, keyBytesLen);
        System.arraycopy(hashDataBytes, 0, ek,prefixLen + keyBytesLen, hashDataBytesLen);
        return ek;
    }

    public static byte[] hashDataKeyPrefixUpperBound(byte[] bytes) {
        int len = bytes.length;
        byte[] buf = new byte[bytes.length];
        System.arraycopy(bytes, 0, buf, 0, bytes.length);
        int i;
        for (i = len - 1; i >= 0; i --) {
            buf[i] ++;
            if (buf[i] != 0) {
                break;
            }
        }
        if (i == -1) {
            System.arraycopy(bytes, 0, buf, 0, bytes.length);
            Arrays.fill(buf, (byte) 0);
        }
        return buf;
    }

    public static byte[] encodeBytes(byte[] data) {
        int dlen = data.length;
        int reallocSize = (dlen/ encGroupSize + 1) * (encGroupSize + 1);
        byte[] result = new byte[reallocSize];
        int loopCnt = 0;
        for (int i = 0; i < dlen; i += encGroupSize)  {
            int remain = dlen -i;
            int padCount = 0;
            if (remain >= encGroupSize) {
                if (i == 0) {
                    System.arraycopy(data, i, result, 0, i + encGroupSize);
                } else {
                    System.arraycopy(data, i, result, i + loopCnt, encGroupSize);
                }
            } else {
                padCount = encGroupSize - remain;
                System.arraycopy(data, i,  result, i + loopCnt, remain) ;
                //byte[] pad = new byte[padCount];
                //Arrays.fill(pad, (byte) 0);
                //System.arraycopy(pad, 0, result, i + 1 + remain, padCount);
            }
            int marker = encMarker - padCount;
            if (loopCnt >= 1) {
                result[(encGroupSize + 1) * (loopCnt + 1) - 1] = (byte) marker;
            } else {
                result[encGroupSize] = (byte) marker;
            }
            loopCnt ++;
        }
        return result;
    }

}
