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

package io.dingodb.exec.transaction.util;

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.Vector;
import io.dingodb.store.api.transaction.data.VectorTableData;
import io.dingodb.store.api.transaction.data.VectorWithId;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

@Slf4j
public class TransactionCacheToMutation {

    public static Mutation cacheToMutation(@Nullable int op, @NonNull byte[] key,
                                           byte[] value, long forUpdateTs,
                                           CommonId tableId, CommonId partId) {
        VectorWithId vectorWithId = null;
        switch (op) {
            case 1:
                vectorWithId = getVectorWithId(key, value, tableId);
                return new Mutation(Op.PUT, key, value, forUpdateTs, vectorWithId);
            case 2:
                vectorWithId = getVectorWithId(key, value, tableId);
                return new Mutation(Op.DELETE, key, value, forUpdateTs, vectorWithId);
            case 3:
                vectorWithId = getVectorWithId(key, value, tableId);
                return new Mutation(Op.PUTIFABSENT, key, value, forUpdateTs, vectorWithId);
            case 5:
                vectorWithId = getVectorWithId(key, value, tableId);
                return new Mutation(Op.LOCK, key, value, forUpdateTs, vectorWithId);
            default:
                log.warn("Op:{},key:{},value:{}", op, key, value);
                return new Mutation(Op.NONE, key, value, forUpdateTs, null);
        }
    }

    private static VectorWithId getVectorWithId(@NonNull byte[] key, byte[] value, CommonId tableId) {
        if (tableId.type.code == 3) {
            Map<CommonId, TableDefinition> indexDefinitions = TransactionUtil.getIndexDefinitions(tableId);
            for (Map.Entry<CommonId, TableDefinition> entry : indexDefinitions.entrySet()) {
                CommonId commonId = entry.getKey();
                TableDefinition tableDefinition = entry.getValue();
                KeyValueCodec keyValueCodec = CodecService.getDefault().createKeyValueCodec(commonId, tableDefinition);
                Object[] record = keyValueCodec.decode(new KeyValue(key, value));
                ColumnDefinition columnDefinition = tableDefinition.getColumns().get(0);
                long longId = Long.parseLong(String.valueOf(record[tableDefinition.getColumnIndex(columnDefinition.getName())]));
                ColumnDefinition columnDefinition1 = tableDefinition.getColumns().get(1);
                Vector vector;
                if (columnDefinition1.getElementType().equalsIgnoreCase("FLOAT")) {
                    List<Float> values = (List<Float>) record[tableDefinition.getColumnIndex(columnDefinition1.getName())];
                    vector = Vector.builder().floatValues(values).valueType(Vector.ValueType.FLOAT).build();
                } else {
                    List<byte[]> values = (List<byte[]>) record[tableDefinition.getColumnIndex(columnDefinition1.getName())];
                    vector = Vector.builder().binaryValues(values).valueType(Vector.ValueType.UINT8).build();
                }
                VectorTableData vectorTableData = new VectorTableData(key, value);
                return VectorWithId.builder()
                    .id(longId)
                    .vector(vector)
                    .tableData(vectorTableData)
                    .build();
            }
        }
        return null;
    }

    public static Mutation cacheToPessimisticLockMutation(@NonNull byte[] key, byte[] value, long forUpdateTs) {
        return new Mutation(Op.LOCK, key, value, forUpdateTs, null);
    }
}
