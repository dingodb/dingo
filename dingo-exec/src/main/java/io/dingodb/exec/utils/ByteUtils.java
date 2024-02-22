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
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.store.api.transaction.data.Op;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

public final class ByteUtils {

    public static final int StartTsLen = 8;
    public static final int ForUpdateTsLen = 8;

    public static final int OpIndex = 2;

    public static final int ExtraNum = 3;

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
            result[result.length - OpIndex] = (byte) code;
        }
        return result;
    }

    /**
     * CommonType | txn | table | part | mutation op | key | value.
     */
    public static Object[] decode(KeyValue keyValue) {
        byte[] bytes = keyValue.getKey();
        Object[] result = new Object[1];
        int from = CommonId.TYPE_LEN;
        CommonId commonId = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        CommonId tableId = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        CommonId partId = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        byte opByte = bytes[bytes.length - OpIndex];
        byte[] destKey = new byte[bytes.length - from];
        System.arraycopy(bytes, from , destKey, 0, destKey.length);
        destKey[destKey.length - OpIndex] = (byte) 0;
        CommonId.CommonType dataType = CommonId.CommonType.of(bytes[0]);
        TxnLocalData txnLocalData;
        if (dataType == CommonId.CommonType.TXN_CACHE_EXTRA_DATA) {
            txnLocalData = TxnLocalData.builder()
                .dataType(dataType)
                .jobId(commonId)
                .tableId(tableId)
                .partId(partId)
                .op(Op.forNumber(opByte))
                .key(destKey)
                .value(keyValue.getValue())
                .build();
        } else {
            txnLocalData = TxnLocalData.builder()
                .dataType(dataType)
                .txnId(commonId)
                .tableId(tableId)
                .partId(partId)
                .op(Op.forNumber(opByte))
                .key(destKey)
                .value(keyValue.getValue())
                .build();
        }
        result[0] = txnLocalData;
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

    public static long decodePessimisticLockValue(KeyValue keyValue) {
        return PrimitiveCodec.decodeLong(keyValue.getValue());
    }

    public static Object[] decodePessimisticExtraKey(byte[] bytes) {
        Object[] result = new Object[1];
        int from = CommonId.TYPE_LEN;
        CommonId commonId = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        CommonId tableId = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        CommonId partId = CommonId.decode(Arrays.copyOfRange(bytes, from, from += CommonId.LEN));
        byte opByte = bytes[bytes.length - OpIndex];
        byte[] destKey = new byte[bytes.length - from];
        System.arraycopy(bytes, from , destKey, 0, destKey.length);
        destKey[destKey.length - OpIndex] = (byte) 0;
        TxnLocalData txnLocalData = TxnLocalData.builder()
            .dataType(CommonId.CommonType.of(bytes[0]))
            .txnId(commonId)
            .tableId(tableId)
            .partId(partId)
            .op(Op.forNumber(opByte))
            .key(destKey)
            .build();
        result[0] = txnLocalData;
        return result;
    }

    public static byte[] decodePessimisticKey(byte[] bytes) {
        int from = CommonId.TYPE_LEN;
        from += CommonId.LEN * ExtraNum;
        byte[] destKey = new byte[bytes.length - from];
        System.arraycopy(bytes, from , destKey, 0, destKey.length);
        destKey[destKey.length - OpIndex] = (byte) 0;
        return destKey;
    }
    public static byte[] getKeyByOp(CommonId.CommonType type, Op op, @NonNull byte[] key) {
        byte[] destKey = Arrays.copyOf(key, key.length);
        destKey[0] = (byte) type.getCode();
        destKey[destKey.length - OpIndex] = (byte) op.getCode();
        return destKey;
    }

    public static Object[] decodeTxnCleanUp(KeyValue keyValue) {
        Object[] result = new Object[1];
        result[0] = keyValue;
        return result;
    }
}
