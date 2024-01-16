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

package io.dingodb.codec.serial;

import io.dingodb.codec.Codec;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.DingoConverter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

@Slf4j
public class DingoKeyValueCodec implements KeyValueCodec {

    private final DingoType schema;
    private final DingoType keySchema;
    TupleMapping keyMapping;
    TupleMapping valueMapping;
    Codec keyCodec;
    Codec valueCodec;

    public DingoKeyValueCodec(@NonNull DingoType schema, TupleMapping keyMapping) {
        this.schema = schema;
        this.keySchema = schema.select(keyMapping);
        this.keyMapping = keyMapping;
        this.valueMapping = keyMapping.inverse(schema.fieldCount());
        keyCodec = new DingoCodec(schema.select(keyMapping).toDingoSchemas(), keyMapping, true);
        valueCodec = new DingoCodec(schema.select(valueMapping).toDingoSchemas(), valueMapping, false);
    }

    @Override
    @SneakyThrows
    public Object[] decode(@NonNull KeyValue keyValue) {
        Object[] record = new Object[keyMapping.size() + valueMapping.size()];
        Object[] key = keyCodec.decodeKey(keyValue.getKey());
        Object[] value = valueCodec.decode(keyValue.getValue());
        for (int i = 0; i < key.length; i++) {
            record[keyMapping.get(i)] = key[i];
        }
        for (int i = 0; i < value.length; i++) {
            record[valueMapping.get(i)] = value[i];
        }
        return (Object[]) schema.convertFrom(record, DingoConverter.INSTANCE);
    }

    @Override
    @SneakyThrows
    public Object[] decodeKey(byte @NonNull [] key) {
        return keyCodec.decodeKey(key);
    }

    @Override
    @SneakyThrows
    public KeyValue encode(Object @NonNull [] tuple) {
        Object[] converted = (Object[]) schema.convertTo(tuple, DingoConverter.INSTANCE);
        Object[] key = new Object[keyMapping.size()];
        Object[] value = new Object[valueMapping.size()];
        for (int i = 0; i < keyMapping.size(); i++) {
            key[i] = converted[keyMapping.get(i)];
        }
        for (int i = 0; i < valueMapping.size(); i++) {
            value[i] = converted[valueMapping.get(i)];
        }
        byte[] keyByte = keyCodec.encodeKey(key);
        byte[] valueByte = valueCodec.encode(value);
        return new KeyValue(keyByte, valueByte);
    }

    @Override
    @SneakyThrows
    public byte[] encodeKey(Object[] tuple) {
        Object[] key = (Object[]) keySchema.convertTo(keyMapping.revMap(tuple), DingoConverter.INSTANCE);
        return keyCodec.encodeKey(key);
    }

    @Override
    @SneakyThrows
    public Object[] mapKeyAndDecodeValue(Object @NonNull [] keys, byte[] bytes) {
        Object[] value = valueCodec.decode(bytes);
        Object[] record = new Object[keyMapping.size() + valueMapping.size()];
        for (int i = 0; i < keys.length; i++) {
            record[keyMapping.get(i)] = keys[i];
        }
        for (int i = 0; i < value.length; i++) {
            record[valueMapping.get(i)] = value[i];
        }
        return (Object[]) schema.convertFrom(record, DingoConverter.INSTANCE);
    }

    @Override
    @SneakyThrows
    public byte[] encodeKeyPrefix(Object[] tuple, int count) {
        if (count == 0) {
            return keyCodec.encodeKeyForRangeScan(tuple, count);
        }
        return keyCodec.encodeKeyForRangeScan(keyMapping.revMap(tuple), count);
    }

    @Override
    @SneakyThrows
    public Object[] decodeKeyPrefix(byte[] keyPrefix) {
        return new Object[0];
    }

}
