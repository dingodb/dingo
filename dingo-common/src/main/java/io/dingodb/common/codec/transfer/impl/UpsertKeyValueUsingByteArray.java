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
import io.dingodb.common.util.Utils;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class UpsertKeyValueUsingByteArray implements KeyValueTransferCodeC {

    public static UpsertKeyValueUsingByteArray INSTANCE = new UpsertKeyValueUsingByteArray();

    /**
     * .
     * input: size|commonId in bytes|len(key)|key|len(value)|value
     * output: object[]
     */
    public Object[] read(ByteBuffer byteBuffer) {
        List<Object> objectArray = new ArrayList<>(5);

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

    /**
     * .
     * input args: CommonId, byte[] key, byte[] value
     * output format:
     *  size|commonId in bytes|len(key)|key|len(value)|value
     */
    public byte[] write(Object[] objectArray) {
        if (objectArray.length != 5) {
            return null;
        }

        CommonId commonId = (CommonId) objectArray[1];
        byte[] commonIdInBytes = commonId == null ? new byte[0] : commonId.encode();

        CommonId tableId = (CommonId) objectArray[2];
        byte[] tableIdInBytes = tableId.encode();

        int keyLen = objectArray[3] == null ? 0 : ((byte[]) objectArray[3]).length;
        int valueLen = objectArray[4] == null ? 0 : ((byte[]) objectArray[4]).length;

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
            .put((byte[]) objectArray[3])
            .putInt(valueLen)
            .put((byte[]) objectArray[4]);
        return byteBuffer.array();
    }
}
