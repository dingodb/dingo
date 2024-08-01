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
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.scalar.BinaryType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.data.Document;
import io.dingodb.store.api.transaction.data.DocumentValue;
import io.dingodb.store.api.transaction.data.DocumentWithId;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.ScalarField;
import io.dingodb.store.api.transaction.data.Vector;
import io.dingodb.store.api.transaction.data.VectorTableData;
import io.dingodb.store.api.transaction.data.VectorWithId;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public final class TransactionCacheToMutation {

    public static final KeyValueCodec CODEC;

    static {
        TupleMapping mapping = TupleMapping.of(new int[]{0});
        DingoType dingoType = new LongType(false);
        TupleType tupleType = DingoTypeFactory.tuple(new DingoType[]{dingoType});
        CODEC = CodecService.getDefault().createKeyValueCodec(1, tupleType, mapping);
    }

    private TransactionCacheToMutation() {
    }

    public static Mutation cacheToMutation(@Nullable int op, @NonNull byte[] key,
                                           byte[] value, long forUpdateTs,
                                           CommonId tableId, CommonId partId, CommonId txnId) {
        VectorWithId vectorWithId = null;
        DocumentWithId documentWithId = null;
        if (tableId.type == CommonId.CommonType.INDEX) {
            IndexTable index = (IndexTable) TransactionManager.getIndex(txnId, tableId);
            if (!index.indexType.isVector && index.indexType != IndexType.DOCUMENT) {
                return new Mutation(Op.forNumber(op), key, value, forUpdateTs, null, null);
            }
            KeyValueCodec keyValueCodec = CodecService.getDefault().createKeyValueCodec(
                index.tableId, index.tupleType(), index.keyMapping()
            );
            Table table = (Table) TransactionManager.getTable(txnId, index.primaryId);
            Object[] record = keyValueCodec.decode(new KeyValue(key, value));
            Object[] tableRecord = new Object[table.columns.size()];
            for (int i = 0; i < record.length; i++) {
                tableRecord[index.getMapping().get(i)] = record[i];
            }
            key = CodecService.getDefault()
                .createKeyValueCodec(table.version, table.tupleType(), table.keyMapping())
                .encodeKey(tableRecord);

            Column column = index.getColumns().get(0);
            List<String> colNames = index.getColumns().stream().map(Column::getName).collect(Collectors.toList());
            long longId = Long.parseLong(String.valueOf(record[colNames.indexOf(column.getName())]));
            if (index.indexType == IndexType.DOCUMENT) {
                Map<String, DocumentValue> documentData = new HashMap<>();
                documentData.put(column.getName(), new DocumentValue(DocumentValue.ScalarFieldType.LONG, new ScalarField(longId)));
                for (int i = 1; i < index.getColumns().size(); i++) {
                    Column columnDef = index.getColumns().get(i);
                    DingoType type = columnDef.type;
                    ScalarField scalarField;
                    DocumentValue.ScalarFieldType fieldType;
                    if (type instanceof BinaryType) {
                        byte[] data = (byte[]) record[colNames.indexOf(columnDef.getName())];
                        fieldType = DocumentValue.ScalarFieldType.BYTES;
                        scalarField = new ScalarField(data);
                    } else if (type instanceof LongType) {
                        Long data = (Long) record[colNames.indexOf(columnDef.getName())];
                        fieldType = DocumentValue.ScalarFieldType.LONG;
                        scalarField = new ScalarField(data);
                    } else if (type instanceof DoubleType) {
                        Double data = (Double) record[colNames.indexOf(columnDef.getName())];
                        fieldType = DocumentValue.ScalarFieldType.DOUBLE;
                        scalarField = new ScalarField(data);
                    } else {
                        String data = (String) record[colNames.indexOf(columnDef.getName())];
                        fieldType = DocumentValue.ScalarFieldType.STRING;
                        scalarField = new ScalarField(data);
                    }
                    DocumentValue documentValue = new DocumentValue(fieldType, scalarField);
                    documentData.put(columnDef.getName(), documentValue);
                }
                documentWithId = DocumentWithId.builder().document(new Document(documentData)).id(longId).build();
            } else {
                Column column1 = index.getColumns().get(1);
                Vector vector;
                if (column1.getElementTypeName().equalsIgnoreCase("FLOAT")) {
                    List<Float> values = (List<Float>) record[colNames.indexOf(column1.getName())];
                    vector = Vector.builder().dimension(values.size()).floatValues(values).valueType(Vector.ValueType.FLOAT).build();
                    record[colNames.indexOf(column1.getName())] = Collections.emptyList();
                } else {
                    List<byte[]> values = (List<byte[]>) record[colNames.indexOf(column1.getName())];
                    vector = Vector.builder().dimension(values.size()).binaryValues(values).valueType(Vector.ValueType.UINT8).build();
                    record[colNames.indexOf(column1.getName())] = Collections.emptyList();
                }
                value = keyValueCodec.encode(record).getValue();
                VectorTableData vectorTableData = new VectorTableData(key, value);
                vectorWithId = VectorWithId.builder()
                    .id(longId)
                    .vector(vector)
                    .tableData(vectorTableData)
                    .build();
                key = CODEC.encodeKeyPrefix(new Object[]{longId}, 1);
            }
        }
        return new Mutation(Op.forNumber(op), key, value, forUpdateTs, vectorWithId, documentWithId);
    }

    public static Mutation cacheToPessimisticLockMutation(@NonNull byte[] key, byte[] value, long forUpdateTs) {
        return new Mutation(Op.LOCK, key, value, forUpdateTs, null, null);
    }
}
