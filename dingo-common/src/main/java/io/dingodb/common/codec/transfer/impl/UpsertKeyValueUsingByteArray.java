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
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class UpsertKeyValueUsingByteArray implements KeyValueTransferCodeC {

    public static UpsertKeyValueUsingByteArray INSTANCE = new UpsertKeyValueUsingByteArray();

    public Object[] read(ByteBuffer byteBuffer) {
        /**
         * input: size|commonId in bytes|len(key)|key|len(value)|value
         * output: object[]
         */
        List<Object> objectArray = new ArrayList<>(3);
        int commonIdLen = byteBuffer.getInt();
        byte[] commonIdInBytes = new byte[commonIdLen];
        byteBuffer.get(commonIdInBytes);

        CommonId commonId = CommonId.fromBytes4Transfer(commonIdInBytes);
        objectArray.add(commonId);

        int keyLen = byteBuffer.getInt();
        byte[] keyInBytes = new byte[keyLen];
        byteBuffer.get(keyInBytes);
        objectArray.add(keyInBytes);

        int valueLen = byteBuffer.getInt();
        byte[] valueInBytes = new byte[valueLen];
        byteBuffer.get(valueInBytes);
        objectArray.add(valueInBytes);

        return objectArray.toArray();
    }

    public byte[] write(Object[] objectArray) {
        /**
         * input args: CommonId, byte[] key, byte[] value
         * output format:
         *  size|commonId in bytes|len(key)|key|len(value)|value
         */
        if (objectArray.length != 3) {
            return null;
        }

        CommonId commonId = (CommonId) objectArray[0];
        byte[] commonIdInBytes = commonId.toBytes4Transfer();

        int keyLen = ((byte[]) objectArray[1]) == null ? 0 : ((byte[]) objectArray[1]).length;
        int valueLen = ((byte[]) objectArray[2]) == null ? 0 : ((byte[]) objectArray[2]).length;

        int totalLen = commonIdInBytes.length + keyLen + valueLen;
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalLen);
        byteBuffer
            .putInt(commonIdInBytes.length)
            .put(commonIdInBytes)
            .putInt(keyLen)
            .put((byte[]) objectArray[1])
            .putInt(valueLen)
            .put((byte[]) objectArray[2]);
        return byteBuffer.array();
    }
}
