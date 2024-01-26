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
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.Vector;
import io.dingodb.store.api.transaction.data.VectorTableData;
import io.dingodb.store.api.transaction.data.VectorWithId;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class TransactionCacheToMutation {

    public static Mutation cacheToMutation(@Nullable int op, @NonNull byte[] key,
                                           byte[] value, long forUpdateTs,
                                           CommonId tableId, CommonId partId) {
        VectorWithId vectorWithId = null;
        if (tableId.type == CommonId.CommonType.INDEX) {
            IndexTable index = TransactionUtil.getIndexDefinitions(tableId);
            KeyValueCodec keyValueCodec = CodecService.getDefault().createKeyValueCodec(
                index.tableId, index.tupleType(), index.keyMapping()
            );
            Object[] record = keyValueCodec.decode(new KeyValue(key, value));
            Column column = index.getColumns().get(0);
            List<String> colNames = index.getColumns().stream().map(Column::getName).collect(Collectors.toList());
            long longId = Long.parseLong(String.valueOf(record[colNames.indexOf(column.getName())]));
            Column column1 = index.getColumns().get(1);
            Vector vector;
            if (column1.getElementTypeName().equalsIgnoreCase("FLOAT")) {
                List<Float> values = (List<Float>) record[colNames.indexOf(column1.getName())];
                vector = Vector.builder().dimension(values.size()).floatValues(values).valueType(Vector.ValueType.FLOAT).build();
            } else {
                List<byte[]> values = (List<byte[]>) record[colNames.indexOf(column1.getName())];
                vector = Vector.builder().dimension(values.size()).binaryValues(values).valueType(Vector.ValueType.UINT8).build();
            }
            VectorTableData vectorTableData = new VectorTableData(key, value);
            vectorWithId = VectorWithId.builder()
                .id(longId)
                .vector(vector)
                .tableData(vectorTableData)
                .build();
            key = Arrays.copyOf(key, key.length -4);
        }
        return new Mutation(Op.forNumber(op), key, value, forUpdateTs, vectorWithId);
    }

    public static Mutation cacheToPessimisticLockMutation(@NonNull byte[] key, byte[] value, long forUpdateTs) {
        return new Mutation(Op.LOCK, key, value, forUpdateTs, null);
    }
}
