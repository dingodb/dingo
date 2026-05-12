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

package io.dingodb.exec.memory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.mysql.MysqlByteUtil;
import io.dingodb.common.store.KeyValue;
import io.dingodb.exec.Services;
import io.dingodb.exec.operator.data.TupleBytesWithJoinFlag;
import io.dingodb.exec.operator.data.TupleWithJoinFlag;
import io.dingodb.exec.operator.params.HashJoinParam;
import io.dingodb.exec.tuple.TupleKey;
import io.dingodb.store.api.StoreInstance;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RocksdbSpiller implements Spiller {

    @Override
    public void spillHashMap(HashJoinParam hashJoinParam) {
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(
            2, 2, hashJoinParam.getRightSchema(), hashJoinParam.getRightMapping()
        );
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(true);
        ObjectMapper objectMapper = hashJoinParam.getObjectMapper();
        for (Map.Entry<TupleKey, List<TupleWithJoinFlag>> entry : hashJoinParam.getHashMap().entrySet()) {
            Object[] keyData = entry.getKey().getTuple();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                objectMapper.writeValue(outputStream, keyData);
                byte[] tupleBytes = outputStream.toByteArray();
                int preLength = hashJoinParam.getJoinId().length;
                List<TupleWithJoinFlag> tupleWithJoinFlagList = entry.getValue();
                for (TupleWithJoinFlag tupleWithJoinFlag : tupleWithJoinFlagList) {
                    byte[] result = new byte[preLength + tupleBytes.length + 8];
                    System.arraycopy(hashJoinParam.getJoinId(), 0, result, 0, preLength);
                    System.arraycopy(tupleBytes, 0, result, preLength, tupleBytes.length);
                    long incVal = hashJoinParam.getInc().incrementAndGet();
                    byte[] incBytes = MysqlByteUtil.longToBytesBigEndian(incVal);
                    System.arraycopy(incBytes, 0, result, preLength + tupleBytes.length, incBytes.length);

                    TupleBytesWithJoinFlag tupleBytesWithJoinFlag = new TupleBytesWithJoinFlag();
                    tupleBytesWithJoinFlag.setInc(incVal);
                    tupleBytesWithJoinFlag.setJoined(tupleWithJoinFlag.isJoined());
                    KeyValue keyValue = codec.encode(tupleWithJoinFlag.getTuple());
                    tupleBytesWithJoinFlag.setTupleBytes(keyValue.getValue());
                    tupleBytesWithJoinFlag.setTupleKeyBytes(keyValue.getKey());
                    localStore.put(new KeyValue(result, objectMapper.writeValueAsBytes(tupleBytesWithJoinFlag)));
                }
                try {
                    outputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void saveSingleKv(TupleKey tupleKey, TupleWithJoinFlag tupleWithJoinFlag,
                             byte[] prefix, HashJoinParam hashJoinParam) {
        ObjectMapper objectMapper = hashJoinParam.getObjectMapper();
        Object[] keyData = tupleKey.getTuple();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        KeyValueCodec codec = hashJoinParam.getCodec();
        if (codec == null) {
            codec = CodecService.getDefault().createKeyValueCodec(
                2, 2, hashJoinParam.getRightSchema(), hashJoinParam.getRightMapping()
            );
        }
        try {
            int preLength = prefix.length;
            objectMapper.writeValue(outputStream, keyData);
            byte[] tupleBytes = outputStream.toByteArray();
            byte[] result = new byte[preLength + tupleBytes.length + 8];
            System.arraycopy(prefix, 0, result, 0, preLength);
            System.arraycopy(tupleBytes, 0, result, preLength, tupleBytes.length);
            long incVal = tupleWithJoinFlag.getInc();
            byte[] incBytes = MysqlByteUtil.longToBytesBigEndian(incVal);
            TupleBytesWithJoinFlag tupleBytesWithJoinFlag = new TupleBytesWithJoinFlag();
            tupleBytesWithJoinFlag.setInc(incVal);
            tupleBytesWithJoinFlag.setJoined(tupleWithJoinFlag.isJoined());
            KeyValue keyValue = codec.encode(tupleWithJoinFlag.getTuple());
            tupleBytesWithJoinFlag.setTupleBytes(keyValue.getValue());
            tupleBytesWithJoinFlag.setTupleKeyBytes(keyValue.getKey());
            System.arraycopy(incBytes, 0, result, preLength + tupleBytes.length, incBytes.length);
            StoreInstance localStore = Services.LOCAL_STORE.getInstance(true);
            localStore.put(new KeyValue(result, objectMapper.writeValueAsBytes(tupleBytesWithJoinFlag)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close(byte[] prefix) {
        StoreInstance cache = Services.LOCAL_STORE.getInstance(null, null);
        Iterator<KeyValue> iterator = cache.scan(prefix);

        while (iterator.hasNext()) {
            cache.delete(iterator.next().getKey());
        }
    }

    public Iterator<TupleWithJoinFlag> getValues(byte[] prefix, HashJoinParam hashJoinParam) {
        StoreInstance cache = Services.LOCAL_STORE.getInstance(true);
        Iterator<KeyValue> iterator = cache.scan(prefix);
        ObjectMapper objectMapper = hashJoinParam.getObjectMapper();
        KeyValueCodec codec = hashJoinParam.getCodec();
        return Iterators.transform(iterator, kv -> {
            TupleBytesWithJoinFlag tupleBytesWithJoinFlag;
            try {
                tupleBytesWithJoinFlag = objectMapper.readValue(kv.getValue(), TupleBytesWithJoinFlag.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Object[] tuples = codec.decode(new KeyValue(tupleBytesWithJoinFlag.getTupleKeyBytes(),
                tupleBytesWithJoinFlag.getTupleBytes()));
            return new TupleWithJoinFlag(tuples,
                tupleBytesWithJoinFlag.isJoined(), tupleBytesWithJoinFlag.getInc());
        });
    }
}
