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

import io.dingodb.common.codec.Codec;
import io.dingodb.common.codec.DingoCodec;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.AvroConverter;

import java.io.IOException;
import javax.annotation.Nonnull;

public class DingoKeyValueCodec implements KeyValueCodec {

    private DingoType schema;
    TupleMapping keyMapping;
    TupleMapping valueMapping;
    Codec keyCodec;
    Codec valueCodec;

    public DingoKeyValueCodec(@Nonnull DingoType schema, @Nonnull TupleMapping keyMapping) {
        this.schema = schema;
        this.keyMapping = keyMapping;
        this.valueMapping = keyMapping.inverse(schema.fieldCount());
//        keyCodec = new AvroCodec(schema.select(keyMapping).toAvroSchema());
//        valueCodec = new AvroCodec(schema.select(valueMapping).toAvroSchema());
        keyCodec = new DingoCodec(schema.select(keyMapping).toDingoSchemas(), keyMapping);
        valueCodec = new DingoCodec(schema.select(valueMapping).toDingoSchemas(), valueMapping);
    }

    @Override
    public Object[] decode(@Nonnull KeyValue keyValue) throws IOException {
        Object[] record = new Object[keyMapping.size() + valueMapping.size()];
        Object[] key = keyCodec.decode(keyValue.getKey());
        Object[] value = valueCodec.decode(keyValue.getValue());
        for (int i = 0; i < key.length; i++) {
            record[keyMapping.get(i)] = key[i];
        }
        for (int i = 0; i < value.length; i++) {
            record[valueMapping.get(i)] = value[i];
        }
        return (Object[]) schema.convertFrom(record, AvroConverter.INSTANCE);
    }

    @Override
    public KeyValue encode(@Nonnull Object[] tuple) throws IOException {
        Object[] converted = (Object[]) schema.convertTo(tuple, AvroConverter.INSTANCE);
        Object[] key = new Object[keyMapping.size()];
        Object[] value = new Object[valueMapping.size()];
        for (int i = 0; i < keyMapping.size(); i++) {
            key[i] = converted[keyMapping.get(i)];
        }
        for (int i = 0; i < valueMapping.size(); i++) {
            value[i] = converted[valueMapping.get(i)];
        }
        byte[] keyByte = keyCodec.encode(key);
        byte[] valueByte = valueCodec.encode(value);
        return new KeyValue(keyByte, valueByte);
    }

    @Override
    public byte[] encodeKey(@Nonnull Object[] keys) throws IOException {
        return keyCodec.encode(keys);
    }

    @Override
    public Object[] mapKeyAndDecodeValue(@Nonnull Object[] keys, byte[] bytes) throws IOException {
        Object[] value = valueCodec.decode(bytes);
        Object[] record = new Object[keyMapping.size() + valueMapping.size()];
        for (int i = 0; i < keys.length; i++) {
            record[keyMapping.get(i)] = keys[i];
        }
        for (int i = 0; i < value.length; i++) {
            record[valueMapping.get(i)] = value[i];
        }
        return (Object[]) schema.convertFrom(record, AvroConverter.INSTANCE);
    }
}
