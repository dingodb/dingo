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
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.store.api.transaction.data.Op;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

public final class ByteUtils {

    public static final int StartTsLen = 8;
    public static final int ForUpdateTsLen = 8;

    public static byte[] encode(CommonId.CommonType commonType, byte[] key, int code, int len, byte[]... bytes) {
        byte[] result = new byte[key.length + len + CommonId.TYPE_LEN];
        key[0] = 't';
        result[0] = (byte) commonType.getCode();
        int destPos = 1;
        for (byte[] idByte : bytes) {
            System.arraycopy(idByte, 0, result, destPos, idByte.length);
            destPos += idByte.length;
        }
        System.arraycopy(key, 0, result, destPos, key.length);
        if (code != 0) {
            result[result.length - 2] = (byte) code;
        }
        return result;
    }
    public static Object[] decode(KeyValue keyValue) {
        byte[] bytes = keyValue.getKey();
        Object[] result = new Object[7];
        int from = 1;
        result[0] = bytes[0];
        result[1] = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        result[2] = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        result[3] = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        result[4] = bytes[bytes.length - 2];
        byte[] destKey = new byte[bytes.length - from];
        System.arraycopy(bytes, from , destKey, 0, destKey.length);
        destKey[destKey.length - 2] = (byte) 0;
        result[5] = destKey;
        result[6] = keyValue.getValue();
        return result;
    }

    public static KeyValue mapping(KeyValue keyValue) {
        byte[] bytes = keyValue.getKey();
        int from = CommonId.TYPE_LEN;
        CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        byte[] destKey = new byte[bytes.length - from];
        System.arraycopy(bytes, from , destKey, 0, destKey.length);
        return new KeyValue(destKey, keyValue.getValue());
    }

    public static byte[] encodePessimisticData(byte[] key, int code, int len, byte[]... bytes) {
        byte[] result = new byte[key.length + len];
        key[0] = 't';
        int destPos = 0;
        for (byte[] idByte : bytes) {
            System.arraycopy(idByte, 0, result, destPos, idByte.length);
            destPos += idByte.length;
        }
        System.arraycopy(key, 0, result, destPos, key.length);
        if (code != 0) {
            result[result.length - 2] = (byte) code;
        }
        return result;
    }

    public static Object[] decodePessimisticLock(KeyValue keyValue) {
        byte[] bytes = keyValue.getKey();
        Object[] result = new Object[7];
        int from = 1;
        result[0] = bytes[0];
        result[1] = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        result[2] = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        result[3] = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        result[4] = bytes[bytes.length - 2];
        byte[] destKey = new byte[bytes.length - from];
        System.arraycopy(bytes, from , destKey, 0, destKey.length);
        destKey[destKey.length - 2] = (byte) 0;
        result[5] = destKey;
        result[6] = PrimitiveCodec.decodeLong(keyValue.getValue());
        return result;
    }

    public static Object[] decodePessimisticExtraKey(byte[] bytes) {
        Object[] result = new Object[6];
        int from = 1;
        result[0] = bytes[0];
        result[1] = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        result[2] = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        result[3] = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        result[4] = bytes[bytes.length - 2];
        byte[] destKey = new byte[bytes.length - from];
        System.arraycopy(bytes, from , destKey, 0, destKey.length);
        destKey[destKey.length - 2] = (byte) 0;
        result[5] = destKey;
        return result;
    }

    public static byte[] encodePessimisticExtraValue(byte[] value, byte newOp) {
        byte[] result = new byte[value.length + 1];
        System.arraycopy(value, 0, result, 0, value.length);
        result[value.length] = newOp;
        return result;
    }

    public static byte[] pessimisticDataToByte(KeyValue keyValue) {
        byte[] result = new byte[keyValue.getKey().length + keyValue.getValue().length];
        System.arraycopy(keyValue.getKey(), 0 , result, 0, keyValue.getKey().length);
        System.arraycopy(keyValue.getValue(), 0 , result, keyValue.getKey().length, keyValue.getValue().length);
        return result;
    }

    public static byte[] getKeyByOp(CommonId.CommonType type, Op op, @NonNull byte[] key) {
        byte[] destKey = Arrays.copyOf(key, key.length);
        destKey[0] = (byte) type.getCode();
        destKey[destKey.length - 2] = (byte) op.getCode();
        return destKey;
    }

    public static Object[] decodeTxnCleanUp(KeyValue keyValue) {
        Object[] result = new Object[1];
        result[0] = keyValue;
        return result;
    }
}
