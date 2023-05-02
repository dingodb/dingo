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
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.DingoConverter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;

public class DingoIndexKeyValueCodec {

    private final DingoType schema;
    private final TupleMapping keyMapping;
    private final TupleMapping indexMapping;
    private final Codec keyCodec;
    private final Codec indexCodec;

    private final boolean unique;

    public DingoIndexKeyValueCodec(@NonNull DingoType schema, TupleMapping keyMapping, TupleMapping indexMapping, boolean unique) {
        this.schema = schema;
        this.keyMapping = keyMapping;
        this.indexMapping = indexMapping;
        keyCodec = new DingoCodec(schema.select(keyMapping).toDingoSchemas(), keyMapping, true);
        indexCodec = new DingoCodec(schema.select(indexMapping).toDingoSchemas(), indexMapping, true);
        this.unique = unique;
    }

    public KeyValue encode(Object[] tuple) throws IOException, ClassCastException {
        Object[] converted = (Object[]) schema.convertTo(tuple, DingoConverter.INSTANCE);
        Object[] key = new Object[keyMapping.size()];
        Object[] value = new Object[indexMapping.size()];
        for (int i = 0; i < keyMapping.size(); i++) {
            key[i] = converted[keyMapping.get(i)];
        }
        for (int i = 0; i < indexMapping.size(); i++) {
            value[i] = converted[indexMapping.get(i)];
        }
        byte[] keyByte = keyCodec.encodeKey(key);
        byte[] indexByte = indexCodec.encodeKey(value);

        if (unique) {
            return new KeyValue(indexByte, keyByte);
        }

        byte[] indexKeyByte = new byte[keyByte.length + indexByte.length];
        System.arraycopy(indexByte, 0, indexKeyByte, 0, indexByte.length);
        System.arraycopy(keyByte, 0, indexKeyByte, indexByte.length, keyByte.length);
        byte[] indexValueByte = encodeInt(keyByte.length);
        return new KeyValue(indexKeyByte, indexValueByte);
    }

    public KeyValue encode(Object[] tuple, byte[] keyByte) throws IOException, ClassCastException {
        Object[] converted = (Object[]) schema.convertTo(tuple, DingoConverter.INSTANCE);
        Object[] value = new Object[indexMapping.size()];
        for (int i = 0; i < indexMapping.size(); i++) {
            value[i] = converted[indexMapping.get(i)];
        }
        byte[] indexByte = indexCodec.encodeKey(value);

        if (unique) {
            return new KeyValue(indexByte, keyByte);
        }

        byte[] indexKeyByte = new byte[keyByte.length + indexByte.length];
        System.arraycopy(keyByte, 0, indexKeyByte, 0, keyByte.length);
        System.arraycopy(indexByte, 0, indexKeyByte, keyByte.length, indexByte.length);
        byte[] indexValueByte = encodeInt(keyByte.length);
        return new KeyValue(indexKeyByte, indexValueByte);
    }

    public byte[] encodeIndexKey(Object[] tuple) throws IOException, ClassCastException {
        Object[] converted = (Object[]) schema.convertTo(tuple, DingoConverter.INSTANCE);
        Object[] key = new Object[indexMapping.size()];
        for (int i = 0; i < indexMapping.size(); i++) {
            key[i] = converted[indexMapping.get(i)];
        }
        return indexCodec.encodeKey(key);
    }

    public byte[] decodeKeyBytes(KeyValue keyValue) throws IOException {
        if (unique) {
            return keyValue.getValue();
        }
        int keyLength = decodeInt(keyValue.getValue());
        byte[] indexKey = new byte[keyLength];
        System.arraycopy(keyValue.getKey(), keyValue.getKey().length - keyLength, indexKey, 0, keyLength);
        return indexKey;
    }

    private byte[] encodeInt(int i) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (i >> 24);
        bytes[1] = (byte) (i >> 16);
        bytes[2] = (byte) (i >> 8);
        bytes[3] = (byte) i;
        return bytes;
    }

    private int decodeInt(byte[] i) {
        return (i[0] & 0xff) << 24 |
            (i[1] & 0xff) << 16 |
            (i[2] & 0xff) << 8 |
            (i[3] & 0xff);
    }
}
