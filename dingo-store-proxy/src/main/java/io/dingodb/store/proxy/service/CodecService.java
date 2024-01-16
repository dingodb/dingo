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

package io.dingodb.store.proxy.service;

import com.google.auto.service.AutoService;
import io.dingodb.codec.CodecServiceProvider;
import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.ListType;
import io.dingodb.common.type.NullableType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.serial.BufImpl;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.utils.TypeSchemaMapper;
import io.dingodb.store.proxy.common.Mapping;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.type.DingoTypeFactory.tuple;
import static io.dingodb.store.proxy.common.Mapping.mapping;

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
        @SneakyThrows
        public Object[] decode(KeyValue keyValue) {
            return (Object[]) type.convertFrom(
                delegate.decode(mapping(CodecService.INSTANCE.setId(keyValue, id))), DingoConverter.INSTANCE
            );
        }

        @Override
        @SneakyThrows
        public Object[] decodeKey(byte @NonNull [] key) {
            throw new UnsupportedEncodingException();
        }

        @Override
        @SneakyThrows
        public KeyValue encode(Object @NonNull [] tuple) {
            return mapping(delegate.encode((Object[]) type.convertTo(tuple, DingoConverter.INSTANCE)));
        }

        @Override
        @SneakyThrows
        public byte[] encodeKey(Object[] tuple) {
            return delegate.encodeKey((Object[]) type.convertTo(tuple, DingoConverter.INSTANCE));
        }

        @Override
        @SneakyThrows
        public byte[] encodeKeyPrefix(Object[] tuple, int count) {
            return delegate.encodeKeyPrefix((Object[]) type.convertTo(tuple, DingoConverter.INSTANCE), count);
        }

        @Override
        @SneakyThrows
        public Object[] decodeKeyPrefix(byte[] keyPrefix) {
            return (Object[]) type.convertFrom(
                delegate.decodeKeyPrefix(CodecService.INSTANCE.setId(keyPrefix, id)), DingoConverter.INSTANCE
            );
        }

    }

    @Override
    public byte[] setId(byte[] key, CommonId id) {
        return setId(key, id.seq);
    }

    public byte[] setId(byte[] key, long id) {
        BufImpl buf = new BufImpl(key);
        // skip namespace
        buf.skip(1);
        // reset id
        buf.writeLong(id);
        return key;
    }

    @Override
    public KeyValueCodec createKeyValueCodec(CommonId id, List<io.dingodb.common.table.ColumnDefinition> columns) {
        return new KeyValueCodec(
            id,
            DingoKeyValueCodec.of(id.seq, columns.stream().map(Mapping::mapping).collect(Collectors.toList())),
            tuple(columns.stream().map(ColumnDefinition::getType).toArray(DingoType[]::new))
        );
    }

    @Override
    public KeyValueCodec createKeyValueCodec(CommonId id, DingoType type, TupleMapping keyMapping) {
        return new KeyValueCodec(id, new DingoKeyValueCodec(id.seq, createSchemasForType(type, keyMapping)), type);
    }

    public static DingoSchema createSchemaForType(DingoType type, int index, boolean isKey) {
        DingoSchema schema;
        if (type instanceof ListType) {
            ListType listType = (ListType) type;
            schema = TypeSchemaMapper.getSchemaForTypeName("LIST", listType.getElementType().getType().toString());
        } else {
            schema = TypeSchemaMapper.getSchemaForTypeName(type.getType().toString(), null);
        }
        schema.setIndex(index);
        schema.setAllowNull(((NullableType)type).isNullable());
        schema.setIsKey(isKey);
        return schema;
    }

    public static List<DingoSchema> createSchemasForType(DingoType type, TupleMapping keyMapping) {
        if (type instanceof TupleType) {
            TupleType tupleType = (TupleType) type;
            DingoType[] fields = tupleType.getFields();
            DingoSchema[] schemas = new DingoSchema[fields.length];
            TupleMapping revMappings = keyMapping.reverse(fields.length);
            int valueIndex = keyMapping.size();
            for (int i = 0; i < fields.length; i++) {
                int primaryIndex = revMappings.get(i);
                if (primaryIndex >= 0) {
                    schemas[primaryIndex] = createSchemaForType(
                        fields[i], keyMapping.get(primaryIndex), true
                    );
                } else {
                    schemas[valueIndex++] = createSchemaForType(fields[i], i, false);
                }
            }
            return Arrays.<DingoSchema>asList(schemas);
        } else {
            return Collections.singletonList(CodecUtils.createSchemaForTypeName(type.getType().toString()));
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
