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

package io.dingodb.client.common;

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.List;

public class KeyValueCodec implements io.dingodb.sdk.common.codec.KeyValueCodec {

    @Getter
    private io.dingodb.sdk.common.codec.KeyValueCodec keyValueCodec;
    @Getter
    private final DingoType dingoType;

    public KeyValueCodec(io.dingodb.sdk.common.codec.KeyValueCodec keyValueCodec, DingoType dingoType) {
        this.keyValueCodec = keyValueCodec;
        this.dingoType = dingoType;
    }

    public KeyValueCodec(io.dingodb.sdk.common.codec.KeyValueCodec keyValueCodec, Table table) {
        this.keyValueCodec = keyValueCodec;
        this.dingoType = getDingoType(table);
    }

    public KeyValueCodec(io.dingodb.sdk.common.codec.KeyValueCodec keyValueCodec, List<Column> columns) {
        this.keyValueCodec = keyValueCodec;
        this.dingoType = getDingoType(columns);
    }

    private static DingoType getDingoType(Table table) {
        return DingoTypeFactory.tuple(
            table.getColumns().stream().map(KeyValueCodec::getDingoType).toArray(DingoType[]::new)
        );
    }

    private static DingoType getDingoType(List<Column> columns) {
        return DingoTypeFactory.tuple(columns.stream().map(KeyValueCodec::getDingoType).toArray(DingoType[]::new));
    }

    private static DingoType getDingoType(Column column) {
        return DingoTypeFactory.INSTANCE.fromName(column.getType(), column.getElementType(), column.isNullable());
    }

    public Object[] decode(KeyValue keyValue) {
        return (Object[]) dingoType.convertFrom(keyValueCodec.decode(keyValue), DingoConverter.INSTANCE);
    }

    @Override
    public Object[] decodeKeyPrefix(byte[] keyPrefix) {
        return (Object[]) dingoType.convertFrom(keyValueCodec.decodeKeyPrefix(keyPrefix), DingoConverter.INSTANCE);
    }

    public KeyValue encode(Object @NonNull [] record) {
        Object[] converted = (Object[]) dingoType.convertTo(record, DingoConverter.INSTANCE);
        return keyValueCodec.encode(converted);
    }

    public byte[] encodeKey(Object[] record) {
        Object[] converted = (Object[]) dingoType.convertTo(record, DingoConverter.INSTANCE);
        return keyValueCodec.encodeKey(converted);
    }

    public byte[] encodeKeyPrefix(Object[] record, int columnCount) {
        Object[] converted = (Object[]) dingoType.convertTo(record, DingoConverter.INSTANCE);
        return keyValueCodec.encodeKeyPrefix(converted, columnCount);
    }

    public byte[] encodeMinKeyPrefix() {
        return keyValueCodec.encodeMinKeyPrefix();
    }

    public byte[] encodeMaxKeyPrefix() {
        return keyValueCodec.encodeMaxKeyPrefix();
    }

    @Override
    public byte[] resetPrefix(byte[] key, long prefix) {
        key = Arrays.copyOf(key, key.length);
        return keyValueCodec.resetPrefix(key, prefix);
    }
}
