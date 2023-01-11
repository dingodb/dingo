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
public class UpsertKeyValueUsingListCodec implements KeyValueTransferCodeC {

    public static UpsertKeyValueUsingListCodec INSTANCE = new UpsertKeyValueUsingListCodec();

    /**
     * .
     * input byteBuffer(the method name has been reed, the position has been seek)
     * @param byteBuffer input byteBuffer
     * @return CommonId, array of KeyValue
     */
    public Object[] read(ByteBuffer byteBuffer) {
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

        int keyValueCnt = byteBuffer.getInt();
        List<KeyValue> keyValueList = null;
        if (keyValueCnt > 0) {
            keyValueList = new ArrayList<>(keyValueCnt);

            for (int i = 0; i < keyValueCnt; i++) {
                int keyLen = byteBuffer.getInt();
                byte[] keyInBytes = new byte[keyLen];
                byteBuffer.get(keyInBytes);

                int valueLen = byteBuffer.getInt();
                byte[] valueInBytes = new byte[valueLen];
                byteBuffer.get(valueInBytes);

                keyValueList.add(new KeyValue(keyInBytes, valueInBytes));
            }
        }
        objectArray.add(keyValueList);
        return objectArray.toArray();
    }

    /**
     * .
     * input args: CommonId, List[KeyValue]
     * output format:
     *  size|commonId in bytes|Count(list element)|len(key)|key|len(value)|value|....
     */
    public byte[] write(Object[] objectArray) {
        if (objectArray.length != 4) {
            return null;
        }

        CommonId commonId = (CommonId) objectArray[1];
        byte[] commonIdInBytes = commonId == null ? new byte[0] : commonId.encode();

        CommonId tableId = (CommonId) objectArray[2];
        byte[] tableIdInBytes = tableId.encode();

        List<KeyValue> keyValueList = (List<KeyValue>) objectArray[3];
        int keyValueCnt = keyValueList.size();

        int totalKeySize = keyValueList
            .stream().map(x -> x == null ? 0 : x.getKey().length).reduce(Integer::sum).get();
        int totalValueSize = keyValueList
            .stream().map(x -> x == null ? 0 : x.getValue().length).reduce(Integer::sum).get();

        /**
         * int(CommonIdSize)|commonId in bytes|Count(list element)|len(key)|key|len(value)|value|....
         */
        int totalLen = (3 * Utils.INTEGER_LEN_IN_BYTES + commonIdInBytes.length + tableIdInBytes.length)
            + (Utils.INTEGER_LEN_IN_BYTES + 2 * Utils.INTEGER_LEN_IN_BYTES * keyValueCnt)
            + totalKeySize + totalValueSize;

        ByteBuffer byteBuffer = ByteBuffer.allocate(totalLen);
        byteBuffer
            .putInt(0)
            .putInt(commonIdInBytes.length)
            .put(commonIdInBytes)
            .putInt(tableIdInBytes.length)
            .put(tableIdInBytes);

        // total key value count
        byteBuffer.putInt(keyValueCnt);

        for (KeyValue keyValue: keyValueList) {
            int keyLen = keyValue.getKey() == null ? 0 : keyValue.getKey().length;
            int valueLen = keyValue.getValue() == null ? 0 : keyValue.getValue().length;
            byteBuffer.putInt(keyLen)
                .put(keyValue.getKey())
                .putInt(valueLen)
                .put(keyValue.getValue());
        }
        return byteBuffer.array();
    }
}
