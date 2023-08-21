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

package io.dingodb.store.service;

import com.google.auto.service.AutoService;
import io.dingodb.codec.CodecServiceProvider;
import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.NullableType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.expr.core.TypeCode;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.serial.BufImpl;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.store.common.Mapping;
import lombok.AllArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.type.DingoTypeFactory.tuple;
import static io.dingodb.store.common.Mapping.mapping;

public final class CodecService implements io.dingodb.codec.CodecService {

    public static final CodecService INSTANCE = new CodecService();

    @AutoService(CodecServiceProvider.class)
    public static final class Provider implements CodecServiceProvider {
        @Override
        public CodecService get() {
            return INSTANCE;
        }
    }

    private CodecService() {
    }

    //
    // Codec service.
    //

    @AllArgsConstructor
    static class KeyValueCodec implements io.dingodb.codec.KeyValueCodec {
        public final CommonId id;
        public final DingoKeyValueCodec delegate;
        public final DingoType type;

        @Override
        public Object[] decode(KeyValue keyValue) throws IOException {
            return (Object[]) type.convertFrom(
                delegate.decode(mapping(CodecService.INSTANCE.setId(keyValue, id))), DingoConverter.INSTANCE
            );
        }

        @Override
        public Object[] decodeKey(byte @NonNull [] bytes) throws IOException {
            throw new UnsupportedEncodingException();
        }

        @Override
        public KeyValue encode(Object @NonNull [] tuple) throws IOException {
            return mapping(delegate.encode((Object[]) type.convertTo(tuple, DingoConverter.INSTANCE)));
        }

        @Override
        public byte[] encodeKey(Object[] keys) throws IOException {
            return delegate.encodeKey((Object[]) type.convertTo(keys, DingoConverter.INSTANCE));
        }

        @Override
        public Object[] mapKeyAndDecodeValue(Object[] keys, byte[] bytes) throws IOException {
            throw new UnsupportedEncodingException();
        }

        @Override
        public byte[] encodeKeyPrefix(Object[] record, int columnCount) throws IOException {
            return delegate.encodeKeyPrefix((Object[]) type.convertTo(record, DingoConverter.INSTANCE), columnCount);
        }

        @Override
        public Object[] decodeKeyPrefix(byte[] keyPrefix) throws IOException {
            return (Object[]) type.convertFrom(
                delegate.decodeKeyPrefix(CodecService.INSTANCE.setId(keyPrefix, id)), DingoConverter.INSTANCE
            );
        }

    }

    @Override
    public byte[] setId(byte[] key, CommonId id) {
        new BufImpl(key).writeLong(id.seq);
        return key;
    }

    @Override
    public KeyValueCodec createKeyValueCodec(CommonId id, List<io.dingodb.common.table.ColumnDefinition> columns
    ) {
        return new KeyValueCodec(
            id,
            DingoKeyValueCodec.of(id.seq, columns.stream().map(Mapping::mapping).collect(Collectors.toList())),
            tuple(columns.stream().map(io.dingodb.common.table.ColumnDefinition::getType).toArray(DingoType[]::new))
        );
    }

    @Override
    public KeyValueCodec createKeyValueCodec(CommonId id, DingoType type, TupleMapping keyMapping) {
        return new KeyValueCodec(id, new DingoKeyValueCodec(id.seq, createSchemasForType(type, keyMapping)), type);
    }

    private static List<DingoSchema> createSchemasForType(DingoType type, TupleMapping keyMapping) {
        if (type instanceof TupleType) {
            TupleType tupleType = (TupleType) type;
            DingoType[] fields = tupleType.getFields();
            DingoSchema[] schemas = new DingoSchema[fields.length];
            int[] mappings = keyMapping.getMappings();
            int valueIndex = mappings.length;
            for (int i = 0; i < fields.length; i++) {
                DingoSchema schema = CodecUtils.createSchemaForTypeName(TypeCode.nameOf(fields[i].getTypeCode()));
                if (keyMapping.contains(i)) {
                    schema.setIsKey(true);
                    schemas[getKeyIndex(mappings, i)] = schema;
                } else {
                    schema.setIsKey(false);
                    schemas[valueIndex++] = schema;
                }
                schema.setIndex(i);
                schema.setAllowNull(((NullableType) fields[i]).isNullable());
            }
            return Arrays.<DingoSchema>asList(schemas);
        } else {
            return Collections.singletonList(CodecUtils.createSchemaForTypeName(TypeCode.nameOf(type.getTypeCode())));
        }
    }

    private static int getKeyIndex(int[] mappings, int index) {
        for (int i = 0; i < mappings.length; i++) {
            if (mappings[i] == index) {
                return i;
            }
        }
        throw new IllegalArgumentException("Not found [" + "] in mappings");
    }
}
