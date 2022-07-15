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
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.AvroConverter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import javax.annotation.Nonnull;


@Slf4j
public class KeyValueCodec {
    private final AvroCodec keyCodec;
    private final AvroCodec valueCodec;
    private final TupleMapping keyMapping;
    private final TupleMapping valueMapping;
    private final DingoType schema;

    public KeyValueCodec(@Nonnull DingoType schema, @Nonnull TupleMapping keyMapping) {
        this.schema = schema;
        this.keyMapping = keyMapping;
        this.valueMapping = keyMapping.inverse(schema.fieldCount());
        keyCodec = new AvroCodec(schema.select(keyMapping).toAvroSchema());
        valueCodec = new AvroCodec(schema.select(valueMapping).toAvroSchema());
    }

    public Object[] decode(@Nonnull KeyValue keyValue) throws IOException {
        Object[] result = new Object[keyMapping.size() + valueMapping.size()];
        keyCodec.decode(result, keyValue.getKey(), keyMapping);
        valueCodec.decode(result, keyValue.getValue(), valueMapping);
        return (Object[]) schema.convertFrom(result, AvroConverter.INSTANCE);
    }

    public KeyValue encode(@Nonnull Object[] tuple) throws IOException {
        Object[] converted = (Object[]) schema.convertTo(tuple, AvroConverter.INSTANCE);
        return new KeyValue(
            keyCodec.encode(converted, keyMapping),
            valueCodec.encode(converted, valueMapping)
        );
    }

    public byte[] encodeKey(@Nonnull Object[] keys) throws IOException {
        return keyCodec.encode(keys);
    }

    public Object[] mapKeyAndDecodeValue(@Nonnull Object[] keys, byte[] bytes) throws IOException {
        Object[] result = new Object[keyMapping.size() + valueMapping.size()];
        keyMapping.map(result, keys);
        valueCodec.decode(result, bytes, valueMapping);
        return (Object[]) schema.convertFrom(result, AvroConverter.INSTANCE);
    }
}
