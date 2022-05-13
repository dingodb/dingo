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

package io.dingodb.common.table;

import io.dingodb.common.codec.AvroCodec;
import io.dingodb.common.store.KeyValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

public class KeyValueCodec {
    private final AvroCodec keyCodec;
    private final AvroCodec valueCodec;
    private final TupleMapping keyMapping;
    private final TupleMapping valueMapping;
    private final TupleSchema tupleSchema;

    public KeyValueCodec(@Nonnull TupleSchema schema, @Nonnull TupleMapping keyMapping) {
        this.tupleSchema = schema;
        this.keyMapping = keyMapping;
        this.valueMapping = keyMapping.inverse(schema.size());
        keyCodec = new AvroCodec(schema.select(keyMapping).getAvroSchema());
        valueCodec = new AvroCodec(schema.select(valueMapping).getAvroSchema());
    }

    private Object[] convertFromAvro(Object[] record) {
        List<Object> result = new ArrayList<>();
        int len = record.length;
        for (int i = 0; i < len; i++) {
            result.add(tupleSchema.get(i).convertFromAvro(record[i]));
        }
        return result.toArray();
    }

    public Object[] decode(@Nonnull KeyValue keyValue) throws IOException {
        Object[] result = new Object[keyMapping.size() + valueMapping.size()];
        keyCodec.decode(result, keyValue.getKey(), keyMapping);
        valueCodec.decode(result, keyValue.getValue(), valueMapping);
        return convertFromAvro(result);
    }

    private Object[] convertToAvro(Object[] record) {
        List<Object> result = new ArrayList<>();
        int len = record.length;
        if (record.length == tupleSchema.size()) {
            for (int i = 0; i < len; i++) {
                result.add(tupleSchema.get(i).convertToAvro(record[i]));
            }
        } else {
            result.add(record[0]);
            for (int i = 1; i < len; i++) {
                result.add(tupleSchema.get(i - 1).convertToAvro(record[i]));
            }
        }
        return result.toArray();
    }


    public KeyValue encode(@Nonnull Object[] record) throws IOException {
        Object[] convertedRecords = convertToAvro(record);
        return new KeyValue(
            keyCodec.encode(convertedRecords, keyMapping),
            valueCodec.encode(convertedRecords, valueMapping)
        );
    }

    public byte[] encodeKey(@Nonnull Object[] keys) throws IOException {
        return keyCodec.encode(keys);
    }

    public byte[] encodeKeyByRecord(@Nonnull Object[] record) throws IOException {
        return keyCodec.encode(record, keyMapping);
    }

    public Object[] mapKeyAndDecodeValue(@Nonnull Object[] keys, byte[] bytes) throws IOException {
        Object[] result = new Object[keyMapping.size() + valueMapping.size()];
        keyMapping.map(result, keys);
        valueCodec.decode(result, bytes, valueMapping);
        return convertFromAvro(result);
    }
}
