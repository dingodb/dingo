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

package io.dingodb.common.codec.transfer.impl;

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.transfer.KeyValueTransferCodeC;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Utils;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class UpsertKeyValueUsingKeyValueCodec implements KeyValueTransferCodeC {

    public static UpsertKeyValueUsingKeyValueCodec INSTANCE = new UpsertKeyValueUsingKeyValueCodec();

    public Object[] read(ByteBuffer byteBuffer) {
        /**
         * input: size|commonId in bytes|len(key)|key|len(value)|value
         * output: object[]
         */
        List<Object> objectArray = new ArrayList<>(4);

        int channelLen = byteBuffer.getInt();
        objectArray.add(null);
        
        int commonIdLen = byteBuffer.getInt();
        if (commonIdLen == 0) {
           objectArray.add(null);
        } else {
            byte[] commonIdInBytes = new byte[commonIdLen];
            byteBuffer.get(commonIdInBytes);

            CommonId commonId = CommonId.decode(commonIdInBytes);
            objectArray.add(commonId);
        }

        int tableIdLen = byteBuffer.getInt();
        byte[] tableIdInBytes = new byte[tableIdLen];
        byteBuffer.get(tableIdInBytes);

        CommonId tableId = CommonId.decode(tableIdInBytes);
        objectArray.add(tableId);

        int keyLen = byteBuffer.getInt();
        byte[] keyInBytes = new byte[keyLen];
        byteBuffer.get(keyInBytes);

        int valueLen = byteBuffer.getInt();
        byte[] valueInBytes = new byte[valueLen];
        byteBuffer.get(valueInBytes);

        KeyValue keyValue = new KeyValue(keyInBytes, valueInBytes);
        objectArray.add(keyValue);

        return objectArray.toArray();
    }

    public byte[] write(Object[] objectArray) {
        /**
         * input args: CommonId, KeyValue
         * output format:
         *  size|commonId in bytes|len(key)|key|len(value)|value|
         */
        if (objectArray.length != 4) {
            return null;
        }

        CommonId commonId = (CommonId) objectArray[1];
        byte[] commonIdInBytes = commonId == null ? new byte[0] : commonId.encode();

        CommonId tableId = (CommonId) objectArray[2];
        byte[] tableIdInBytes = tableId.encode();

        KeyValue keyValue = (KeyValue) objectArray[3];
        int keyLen = (keyValue.getKey() == null) ? 0 : (keyValue.getKey().length);
        int valueLen = (keyValue.getKey() == null) ? 0 : (keyValue.getValue().length);

        int totalLen = 5 * Utils.INTEGER_LEN_IN_BYTES + commonIdInBytes.length
            + tableIdInBytes.length + keyLen + valueLen;
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalLen);
        byteBuffer
            .putInt(0)
            .putInt(commonIdInBytes.length)
            .put(commonIdInBytes)
            .putInt(tableIdInBytes.length)
            .put(tableIdInBytes)
            .putInt(keyLen)
            .put(keyValue.getKey())
            .putInt(valueLen)
            .put(keyValue.getValue());
        return byteBuffer.array();
    }
}
