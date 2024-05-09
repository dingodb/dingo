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

package io.dingodb.codec;

import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;

import java.util.Arrays;
import java.util.List;

public interface CodecService {

    static CodecService getDefault() {
        return CodecServiceProvider.getDefault().get();
    }

    //  [namespace(1)|id(8)]
    default byte[] empty() {
        return new byte[9];
    }

    default byte[] setId(byte[] key, CommonId id) {
        return key;
    }

    default byte[] setId(byte[] key, long id) {
        return key;
    }

    default KeyValue setId(KeyValue keyValue, CommonId id) {
        return new KeyValue(setId(keyValue.getKey(), id), keyValue.getValue());
    }

    KeyValueCodec createKeyValueCodec(int version, CommonId id, DingoType type, TupleMapping keyMapping);

    default KeyValueCodec createKeyValueCodec(int version, CommonId id, List<ColumnDefinition> columns) {
        int[] mappings = new int[columns.size()];
        int keyCount = 0;
        for (int i = 0; i < columns.size(); i++) {
            int primaryKeyIndex = columns.get(i).getPrimary();
            if (primaryKeyIndex >= 0) {
                mappings[primaryKeyIndex] = i;
                keyCount++;
            }
        }
        TupleMapping keyMapping = TupleMapping.of(Arrays.copyOf(mappings, keyCount));
        return createKeyValueCodec(
            version,
            id,
            DingoTypeFactory.tuple((DingoType[]) columns.stream().map(ColumnDefinition::getType).toArray()),
            keyMapping
        );
    }

    @Deprecated
    default KeyValueCodec createKeyValueCodec(CommonId id, DingoType type, TupleMapping keyMapping) {
        return createKeyValueCodec(1, id, type, keyMapping);
    }

    @Deprecated
    default KeyValueCodec createKeyValueCodec(CommonId id, List<ColumnDefinition> columns) {
        return createKeyValueCodec(1, id, columns);
    }

    default KeyValueCodec createKeyValueCodec(CommonId id, TableDefinition tableDefinition) {
        return createKeyValueCodec(tableDefinition.getVersion(), id, tableDefinition.getColumns());
    }

    default KeyValueCodec createKeyValueCodec(int version, DingoType type, TupleMapping keyMapping) {
        return createKeyValueCodec(version, CommonId.EMPTY_TABLE, type, keyMapping);
    }

    default KeyValueCodec createKeyValueCodec(int version, List<ColumnDefinition> columns) {
        return createKeyValueCodec(version, CommonId.EMPTY_TABLE, columns);
    }

    default KeyValueCodec createKeyValueCodec(TableDefinition tableDefinition) {
        return createKeyValueCodec(tableDefinition.getVersion(), CommonId.EMPTY_TABLE, tableDefinition.getColumns());
    }

}
