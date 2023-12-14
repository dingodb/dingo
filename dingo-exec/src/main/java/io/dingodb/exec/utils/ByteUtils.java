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

package io.dingodb.exec.utils;

import io.dingodb.common.CommonId;

import java.util.Arrays;

public final class ByteUtils {

    public static byte[] encode(byte[] key, int code, int len, byte[]... bytes) {
        byte[] result = new  byte[key.length + len];
        int destPos = 0;
        for (byte[] idByte : bytes) {
            System.arraycopy(idByte, 0, result, destPos, idByte.length);
            destPos += idByte.length;
        }
        System.arraycopy(key, 0, result, destPos, key.length);
        result[result.length -2] = (byte) code;
        return result;
    }

    public static Object[] decode(byte[] bytes) {
        Object[] result = new Object[4];
        int from = 0;
        result[0] = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        result[1] = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        result[2] = CommonId.decode(Arrays.copyOfRange(bytes, from, from + CommonId.LEN));
        result[3] = bytes[bytes.length - 2];
        return result;
    }
}
