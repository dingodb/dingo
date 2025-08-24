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

package io.dingodb.exec.operator;

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.exception.DingoTypeRangeException;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnPartReplaceIntoParam;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.base.TxnPartData;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.exec.utils.OpStateUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TxnPartReplaceIntoOperator extends PartModifyOperator {
    public static final TxnPartReplaceIntoOperator INSTANCE = new TxnPartReplaceIntoOperator();

    private TxnPartReplaceIntoOperator() {
    }

    @Override
    protected boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        TxnPartReplaceIntoParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("partReplaceIntoLocal");
        long start = System.currentTimeMillis();
        if (param.isHasAutoInc() && param.getAutoIncColIdx() < tuple.length) {
            Object tmp = tuple[param.getAutoIncColIdx()];
            if (tmp instanceof Long || tmp instanceof Integer) {
                long autoIncVal = Long.parseLong(tmp.toString());
                MetaService metaService = MetaService.root();
                metaService.updateAutoIncrement(param.getTableId(), autoIncVal);
                param.getAutoIncList().add(autoIncVal);
            }
        }
        param.setContext(context);
        CommonId tableId = param.getTableId();
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId partId = context.getDistribution().getId();
        DingoType schema = param.getSchema();
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
        KeyValueCodec codec = param.getCodec();
        boolean isVector = false;
        boolean isDocument = false;
        boolean isUnique = false;
        Table indexTable = null;
        Object[] copyTuple = null;
        //Only for origin table.
        if (context.getIndexId() == null && !param.isPessimisticTxn()) {
            List<Column> originColumns = ((Table) TransactionManager.getTable(txnId, tableId)).getColumns();
            for (int i = 0; i < originColumns.size(); i++) {
                DingoType t = originColumns.get(i).getType();
                if (t instanceof io.dingodb.common.type.scalar.DecimalType) {
                    if (tuple[i] instanceof BigDecimal) {
                        long valueIntPart = ((BigDecimal) tuple[i]).precision() - ((BigDecimal) tuple[i]).scale();
                        long typeIntPart = ((io.dingodb.common.type.scalar.DecimalType) t).getPrecision()
                            - ((io.dingodb.common.type.scalar.DecimalType) t).getScale();
                        if (valueIntPart > typeIntPart) {
                            throw new DingoTypeRangeException(0, "Out of range value for column '" + originColumns.get(i).getName() + "'");
                        }
                    }
                }
            }
        }

        IndexTable index = null;
        if (context.getIndexId() != null) {
            indexTable = (Table) TransactionManager.getIndex(txnId, context.getIndexId());
            if (indexTable == null) {
                LogUtils.error(log, "[ddl] TxnPartInsert get index table null, indexId:{}", context.getIndexId());
                return false;
            }
            if (!OpStateUtils.allowWrite(indexTable.getSchemaState())) {
                return true;
            }
            List<Integer> columnIndices = param.getTable().getColumnIndices(indexTable.columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList()));
            tableId = context.getIndexId();
            Object defaultVal = null;
            if (columnIndices.contains(-1)) {
                Column addColumn = indexTable.getColumns().stream()
                    .filter(column -> column.getSchemaState() != SchemaState.SCHEMA_PUBLIC)
                    .findFirst().orElse(null);
                if (addColumn != null) {
                    defaultVal = addColumn.getDefaultVal();
                }
            }
            copyTuple = Arrays.copyOf(tuple, tuple.length);
            Object[] finalTuple = tuple;
            Object finalDefaultVal = defaultVal;
            tuple = columnIndices.stream().map(i -> {
                if (i == -1) {
                    return finalDefaultVal;
                }
                return finalTuple[i];
            }).toArray();
            index = (IndexTable) TransactionManager.getIndex(txnId, tableId);
            if (index.indexType.isVector) {
                isVector = true;
            }
            if (index.indexType == IndexType.DOCUMENT) {
                isDocument = true;
            }
            isUnique = index.unique;
            schema = indexTable.tupleType();
            localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
            codec = CodecService.getDefault().createKeyValueCodec(
                indexTable.getCodecVersion(), indexTable.version, indexTable.tupleType(), indexTable.keyMapping()
            );
        }
        Object[] newTuple = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
        KeyValue keyValue = wrap(codec::encode).apply(newTuple);
        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
        byte[] key = keyValue.getKey();
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = tableId.encode();
        byte[] partIdByte = partId.encode();
        byte[] jobIdByte = vertex.getTask().getJobId().encode();
        int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
        if (param.isPessimisticTxn()) {
            byte[] keyValueKey = keyValue.getKey();
            // dataKeyValue   [10_txnId_tableId_partId_a_putIf, value]
            byte[] dataKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                keyValueKey,
                Op.PUTIFABSENT.getCode(),
                len,
                txnIdByte, tableIdByte, partIdByte);
            byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
            deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
            byte[] updateKey = Arrays.copyOf(dataKey, dataKey.length);
            updateKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
            List<byte[]> bytes = new ArrayList<>(3);
            bytes.add(dataKey);
            bytes.add(deleteKey);
            bytes.add(updateKey);
            List<KeyValue> keyValues = localStore.get(bytes);
            if (keyValues != null && !keyValues.isEmpty()) {
                if (keyValues.size() > 1) {
                    throw new RuntimeException(txnId + " PrimaryKey is not existed than two in local store");
                }
                KeyValue value = keyValues.get(0);
                byte[] oldKey = value.getKey();
                long num = 1L;
                if (oldKey[oldKey.length - 2] == Op.PUTIFABSENT.getCode()
                    || oldKey[oldKey.length - 2] == Op.PUT.getCode()) {
                    num++;
                }
                byte[] extraKey = ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                    key,
                    oldKey[oldKey.length - 2],
                    len,
                    jobIdByte,
                    tableIdByte,
                    partIdByte
                );
                KeyValue extraKeyValue;
                if (value.getValue() == null) {
                    // delete
                    extraKeyValue = new KeyValue(extraKey, null);
                } else {
                    extraKeyValue = new KeyValue(
                        extraKey, Arrays.copyOf(value.getValue(), value.getValue().length)
                    );
                }
                localStore.put(extraKeyValue);
                localStore.delete(dataKey);
                localStore.delete(deleteKey);
                localStore.delete(updateKey);
                vertex.getTask().getPartData().put(
                    new TxnPartData(tableId, partId),
                    (!isVector && !isDocument)
                );
                keyValue.setKey(updateKey);
                if (localStore.put(keyValue) && context.getIndexId() == null) {
                    param.inc(num);
                }
            } else {
                byte[] rollBackKey = ByteUtils.getKeyByOp(
                    CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey
                );
                if (localStore.get(rollBackKey) != null) {
                    localStore.delete(rollBackKey);
                }
                // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
                byte[] extraKey = ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                    key,
                    Op.NONE.getCode(),
                    len,
                    jobIdByte,
                    tableIdByte,
                    partIdByte
                );
                localStore.put(new KeyValue(extraKey, Arrays.copyOf(keyValue.getValue(), keyValue.getValue().length)));
                long num = 1L;
                // write data
                if (context.isReplaceIntoKey()) {
                    num++;
                    keyValue.setKey(updateKey);
                } else {
                    keyValue.setKey(dataKey);
                }
                vertex.getTask().getPartData().put(
                    new TxnPartData(tableId, partId),
                    (!isVector && !isDocument)
                );
                if (localStore.put(keyValue) && context.getIndexId() == null) {
                    param.inc(num);
                }
            }
        } else {
            byte[] insertKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                keyValue.getKey(),
                Op.PUTIFABSENT.getCode(),
                (txnIdByte.length + tableIdByte.length + partIdByte.length),
                txnIdByte,
                tableIdByte,
                partIdByte);
            byte[] deleteKey = Arrays.copyOf(insertKey, insertKey.length);
            deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
            byte[] updateKey = Arrays.copyOf(insertKey, insertKey.length);
            updateKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
            List<byte[]> bytes = new ArrayList<>(3);
            bytes.add(insertKey);
            bytes.add(deleteKey);
            bytes.add(updateKey);
            List<KeyValue> keyValues = localStore.get(bytes);
            Op op = Op.NONE;
            long num = 1L;
            if (keyValues != null && !keyValues.isEmpty()) {
                if (keyValues.size() > 1) {
                    throw new RuntimeException(txnId + " Key is not existed than two in local store");
                }
                KeyValue value = keyValues.get(0);
                byte[] oldKey = value.getKey();
                if (oldKey[oldKey.length - 2] == Op.PUTIFABSENT.getCode()
                    || oldKey[oldKey.length - 2] == Op.PUT.getCode()) {
                    num++;
                    Object[] decode = ByteUtils.decode(value);
                    TxnLocalData txnLocalData = (TxnLocalData) decode[0];
                    KeyValue kvKeyValue = new KeyValue(txnLocalData.getKey(), value.getValue());
                    CodecService.getDefault().setId(kvKeyValue.getKey(), txnLocalData.getPartId().domain);
                    if (isUnique) {
                        // 1、Find the corresponding main table data and delete it.
                        // 2、The current index data does not necessarily coincide with the index data inserted
                        // in the corresponding main table at that time, so you also need to
                        // find the corresponding index data of the current main table and delete it.
                        // (1,'lisi',28,180,null),
                        // (2,'lisisi',157,280,0);
                        // replace (2,'lisi',33,178,1)
                        // bug-3012, 1: row 2 lisi put, 2: list 2 put -> list 1 delete,
                        // 3: row 1 list delete and delete all index data,
                        // 4: row 2 lisisi 2 delete and delete all index data
                        List<String> columnNames = index.getColumns().stream().map(Column::getName).toList();
                        Table tableDefinition = param.getTable();
                        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
                            MetaService.root().getRangeDistribution(tableDefinition.tableId);
                        // 3: 1 list delete and delete all index data
                        deletePrimaryKeyByUniqueIndex(
                            vertex,
                            param,
                            columnNames,
                            tableDefinition,
                            ranges,
                            txnId,
                            codec,
                            isVector,
                            isDocument,
                            txnIdByte,
                            jobIdByte,
                            len,
                            Op.forNumber(oldKey[oldKey.length - 2]),
                            kvKeyValue,
                            copyTuple,
                            context,
                            true
                        );
                        // 4: lisisi 2 delete
                        deleteUniqueIndexByCurrentPrimaryKey(
                            context,
                            vertex,
                            param,
                            tableId,
                            txnId,
                            codec,
                            isVector,
                            isDocument,
                            indexTable,
                            copyTuple,
                            txnIdByte,
                            tableIdByte,
                            jobIdByte,
                            len,
                            Op.forNumber(oldKey[oldKey.length - 2]),
                            ranges,
                            true
                        );
                    } else if (context.getIndexId() == null) {
                        // delete all index data
                        for (IndexTable currentIndex : param.getTable().getIndexes()) {
                            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution =
                                MetaService.root().getRangeDistribution(currentIndex.tableId);
                            deleteIndexData(
                                vertex,
                                param,
                                isVector,
                                isDocument,
                                txnIdByte,
                                jobIdByte,
                                len,
                                op,
                                kvKeyValue,
                                currentIndex,
                                rangeDistribution,
                                true
                            );
                        }
                    }
                } else {
                    // delete  ->  insert  convert --> put
                    insertKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
                }
            } else {
                byte[] originalKey;
                if (isVector) {
                    originalKey = codec.encodeKeyPrefix(newTuple, 1);
                    CodecService.getDefault().setId(originalKey, partId.domain);
                } else if (isDocument) {
                    originalKey = codec.encodeKeyPrefix(newTuple, 1);
                    CodecService.getDefault().setId(originalKey, partId.domain);
                } else {
                    originalKey = key;
                }
                StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
                // index list is primaryKey
                KeyValue kvKeyValue = kvStore.txnGet(
                    txnId.seq,
                    originalKey,
                    param.getLockTimeOut()
                );
                if (kvKeyValue != null && kvKeyValue.getValue() != null) {
                    num++;
                    if (isUnique) {
                        // 1、Find the corresponding main table data and delete it.
                        // 2、The current index data does not necessarily coincide with the index data inserted
                        // in the corresponding main table at that time, so you also need to
                        // find the corresponding index data of the current main table and delete it.
                        // (1,'lisi',28,180,null),
                        // (2,'lisisi',157,280,0);
                        // replace (2,'lisi',33,178,1)
                        // bug-3012, 1: row 2 lisi put, 2: list 2 put -> list 1 delete,
                        // 3: row 1 list delete and delete all index data,
                        // 4: row 2 lisisi 2 delete and delete all index data
                        List<String> columnNames = index.getColumns().stream().map(Column::getName).toList();
                        Table tableDefinition = param.getTable();
                        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
                            MetaService.root().getRangeDistribution(tableDefinition.tableId);
                        // 3: 1 list delete and delete all index data
                        deletePrimaryKeyByUniqueIndex(
                            vertex,
                            param,
                            columnNames,
                            tableDefinition,
                            ranges,
                            txnId,
                            codec,
                            isVector,
                            isDocument,
                            txnIdByte,
                            jobIdByte,
                            len,
                            op,
                            kvKeyValue,
                            copyTuple,
                            context,
                            false
                        );
                        // 4: lisisi 2 delete
                        deleteUniqueIndexByCurrentPrimaryKey(
                            context,
                            vertex,
                            param,
                            tableId,
                            txnId,
                            codec,
                            isVector,
                            isDocument,
                            indexTable,
                            copyTuple,
                            txnIdByte,
                            tableIdByte,
                            jobIdByte,
                            len,
                            op,
                            ranges,
                            false
                        );
                    } else if (context.getIndexId() == null) {
                        // delete all index data
                        for (IndexTable currentIndex : param.getTable().getIndexes()) {
                            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution =
                                MetaService.root().getRangeDistribution(currentIndex.tableId);
                            deleteIndexData(
                                vertex,
                                param,
                                isVector,
                                isDocument,
                                txnIdByte,
                                jobIdByte,
                                len,
                                op,
                                kvKeyValue,
                                currentIndex,
                                rangeDistribution,
                                false
                            );
                        }
                    }
                }
            }
            keyValue.setKey(ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_DATA, Op.PUT, insertKey));
            localStore.delete(insertKey);
            localStore.delete(updateKey);
            localStore.delete(deleteKey);
            // for optimistic transaction for update
            byte[] rollbackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_DATA, Op.ROLLBACK, deleteKey);
            if (context.getIndexId() == null && localStore.get(rollbackKey) != null ) {
                localStore.delete(rollbackKey);
                op = Op.ROLLBACK;
            }
            // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
            byte[] extraKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                key,
                op.getCode(),
                len,
                jobIdByte,
                tableIdByte,
                partIdByte
            );
            vertex.getTask().getPartData().put(
                new TxnPartData(tableId, partId),
                (!isVector && !isDocument)
            );
            localStore.put(new KeyValue(extraKey, Arrays.copyOf(keyValue.getValue(), keyValue.getValue().length)));
            if (localStore.put(keyValue) && context.getIndexId() == null) {
                param.inc(num);
                context.addKeyState(true);
            }
        }
        profile.time(start - System.currentTimeMillis());
        return true;
    }

    private static void deleteUniqueIndexByCurrentPrimaryKey(
                             Context context,
                             Vertex vertex,
                             TxnPartReplaceIntoParam param,
                             CommonId tableId,
                             CommonId txnId,
                             KeyValueCodec codec,
                             boolean isVector,
                             boolean isDocument,
                             Table indexTable,
                             Object[] copyTuple,
                             byte[] txnIdByte,
                             byte[] tableIdByte,
                             byte[] jobIdByte,
                             int len,
                             Op op,
                             NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges,
                             boolean isLocal) {
        StoreInstance kvStore;
        CommonId regionId;
        // 4: lisisi 2 delete ->  by Current primary key :2 get unique index lisisi 2
        // Current primary key
        byte[] keyEncode = param.getCodec().encodeKey(copyTuple);
        regionId = PartitionService.getService(
                Optional.ofNullable(param.getTable().getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(keyEncode, ranges);
        KeyValue currentPrimaryKeyValue;
        if (!isLocal) {
            kvStore = Services.KV_STORE.getInstance(tableId, regionId);
            keyEncode = CodecService.getDefault().setId(keyEncode, regionId.domain);
            currentPrimaryKeyValue = kvStore.txnGet(
                txnId.seq,
                keyEncode,
                param.getLockTimeOut()
            );
        } else {
            byte[] regionIdByte = regionId.encode();
            byte[] primaryKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                keyEncode,
                op.getCode(),
                (txnIdByte.length + tableIdByte.length + regionIdByte.length),
                txnIdByte,
                tableIdByte,
                regionIdByte
            );
            StoreInstance primaryKeyLocalStore = Services.LOCAL_STORE.getInstance(tableId, regionId);
            currentPrimaryKeyValue = primaryKeyLocalStore.get(primaryKey);
        }

        if (currentPrimaryKeyValue != null && currentPrimaryKeyValue.getValue() != null) {
            // delete all index data
            for (IndexTable index : param.getTable().getIndexes()) {
                NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution =
                    MetaService.root().getRangeDistribution(index.tableId);
                deleteIndexData(
                    vertex,
                    param,
                    isVector,
                    isDocument,
                    txnIdByte,
                    jobIdByte,
                    len,
                    op,
                    currentPrimaryKeyValue,
                    index,
                    rangeDistribution,
                    isLocal
                );
            }
        }
    }

    private static void deletePrimaryKeyByUniqueIndex(Vertex vertex,
                                  TxnPartReplaceIntoParam param,
                                  List<String> columnNames,
                                  Table tableDefinition,
                                  NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges,
                                  CommonId txnId,
                                  KeyValueCodec codec,
                                  boolean isVector,
                                  boolean isDocument,
                                  byte[] txnIdByte,
                                  byte[] jobIdByte,
                                  int len,
                                  Op op,
                                  KeyValue kvKeyValue,
                                  Object[] copyTuple,
                                  Context context,
                                  boolean isLocal) {
        // 3: 1 list delete and delete all index data
        // list 1
        StoreInstance kvStore;
        CommonId tableId = param.getTableId();
        byte[] tableIdByte = tableId.encode();
        Object[] uniqueTuple = codec.decode(kvKeyValue);
        TupleMapping tupleMapping = TupleMapping.of(tableDefinition.getColumnIndices(columnNames));
        Object[] keyTuples = new Object[tableDefinition.getColumns().size()];
        for (int i = 0; i < tupleMapping.getMappings().length; i ++) {
            keyTuples[tupleMapping.get(i)] = uniqueTuple[i];
        }
        // 1 list
        byte[] keys = param.getCodec().encodeKey(keyTuples);
        CommonId regionId = PartitionService.getService(
                Optional.ofNullable(tableDefinition.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(keys, ranges);
        keys = CodecService.getDefault().setId(keys, regionId.domain);
        // current primary key
        byte[] keyEncode = param.getCodec().encodeKey(copyTuple);
        regionId = PartitionService.getService(
                Optional.ofNullable(param.getTable().getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(keyEncode, ranges);
        keyEncode = CodecService.getDefault().setId(keyEncode, regionId.domain);
        // key same
        if (ByteArrayUtils.compare(keys, keyEncode, 0) == 0) {
            return;
        }
        byte[] regionIdByte = regionId.encode();
        KeyValue primaryKeyValue;
        if (!isLocal) {
            kvStore = Services.KV_STORE.getInstance(tableId, regionId);
            // use 1 primaryKey get main table data
            primaryKeyValue = kvStore.txnGet(
                txnId.seq,
                keys,
                param.getLockTimeOut()
            );
        } else {
            byte[] primaryKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                keys,
                op.getCode(),
                (txnIdByte.length + tableIdByte.length + regionIdByte.length),
                txnIdByte,
                tableIdByte,
                regionIdByte
            );
            StoreInstance primaryKeyLocalStore = Services.LOCAL_STORE.getInstance(tableId, regionId);
            primaryKeyValue = primaryKeyLocalStore.get(primaryKey);
        }
        // delete main table data: 1 list delete
        if (primaryKeyValue != null && primaryKeyValue.getValue() != null) {
            byte[] primaryKey;
            if (!isLocal) {
                primaryKey = primaryKeyValue.getKey();
            } else {
                StoreInstance primaryKeyLocalStore = Services.LOCAL_STORE.getInstance(tableId, regionId);
                primaryKeyLocalStore.delete(primaryKeyValue.getKey());
                Object[] decode = ByteUtils.decode(primaryKeyValue);
                TxnLocalData txnLocalData = (TxnLocalData) decode[0];
                primaryKey = txnLocalData.getKey();
            }
            byte[] deletePrimaryKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                primaryKey,
                Op.DELETE.getCode(),
                (txnIdByte.length + tableIdByte.length + regionIdByte.length),
                txnIdByte,
                tableIdByte,
                regionIdByte
            );
            // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
            byte[] extraKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                primaryKey,
                op.getCode(),
                len,
                jobIdByte,
                tableIdByte,
                regionIdByte
            );
            vertex.getTask().getPartData().put(
                new TxnPartData(tableId, regionId),
                (!isVector && !isDocument)
            );
            StoreInstance primaryKeyLocalStore = Services.LOCAL_STORE.getInstance(tableId, regionId);
            primaryKeyLocalStore.put(new KeyValue(extraKey,
                Arrays.copyOf(primaryKeyValue.getValue(), primaryKeyValue.getValue().length))
            );
            if (primaryKeyLocalStore.put(new KeyValue(deletePrimaryKey,
                Arrays.copyOf(primaryKeyValue.getValue(), primaryKeyValue.getValue().length)))) {
                param.inc(1L);
            }
            // delete all index data
            for (IndexTable index : param.getTable().getIndexes()) {
                if (index.tableId == context.getIndexId()) {
                    continue;
                }
                NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution =
                    MetaService.root().getRangeDistribution(index.tableId);
                deleteIndexData(
                    vertex,
                    param,
                    isVector,
                    isDocument,
                    txnIdByte,
                    jobIdByte,
                    len,
                    op,
                    primaryKeyValue,
                    index,
                    rangeDistribution,
                    isLocal
                );
            }

        } else {
            throw new RuntimeException("The data in the primary table corresponding to " +
                "the unique index does not exist， primary key is " + Arrays.toString(keys));
        }
    }

    private static void deleteIndexData(Vertex vertex,
                                        TxnPartReplaceIntoParam param,
                                        boolean isVector,
                                        boolean isDocument,
                                        byte[] txnIdByte,
                                        byte[] jobIdByte,
                                        int len,
                                        Op op,
                                        KeyValue primaryKeyValue,
                                        IndexTable index,
                                        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution,
                                        boolean isLocal) {
        CommonId regionId;
        CommonId tableId;
        byte[] regionIdByte;
        List<Integer> columnIndices = param.getTable().getColumnIndices(index.columns.stream()
            .map(Column::getName)
            .collect(Collectors.toList()));
        tableId = index.tableId;
        byte[] tableIdByte = tableId.encode();
        Object defaultVal = null;
        if (columnIndices.contains(-1)) {
            Column addColumn = index.getColumns().stream()
                .filter(column -> column.getSchemaState() != SchemaState.SCHEMA_PUBLIC)
                .findFirst().orElse(null);
            if (addColumn != null) {
                defaultVal = addColumn.getDefaultVal();
            }
        }
        Object[] finalTuple = param.getCodec().decode(primaryKeyValue);
        Object finalDefaultVal = defaultVal;
        Object[] oldIndexTuple = columnIndices.stream().map(i -> {
            if (i == -1) {
                return finalDefaultVal;
            }
            return finalTuple[i];
        }).toArray();
        // get index data
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(
            index.getCodecVersion(), index.version, index.tupleType(), index.keyMapping()
        );
        KeyValue oldKeyValue = wrap(codec::encode).apply(oldIndexTuple);
        regionId = PartitionService.getService(
                Optional.ofNullable(index.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(oldKeyValue.getKey(), rangeDistribution);
        byte[] oldKeys = CodecService.getDefault().setId(oldKeyValue.getKey(), regionId.domain);
        regionIdByte = regionId.encode();
        byte[] deletePrimaryKey = ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_DATA,
            oldKeys,
            Op.DELETE.getCode(),
            (txnIdByte.length + tableIdByte.length + regionIdByte.length),
            txnIdByte,
            tableIdByte,
            regionIdByte
        );
        // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
        byte[] extraKey = ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
            oldKeys,
            op.getCode(),
            len,
            jobIdByte,
            tableIdByte,
            regionIdByte
        );
        vertex.getTask().getPartData().put(
            new TxnPartData(tableId, regionId),
            (!isVector && !isDocument)
        );
        StoreInstance primaryKeyLocalStore = Services.LOCAL_STORE.getInstance(tableId, regionId);
        if (isLocal) {
            primaryKeyLocalStore.delete(ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_DATA, Op.PUT, oldKeys));
            primaryKeyLocalStore.delete(ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_DATA, Op.PUTIFABSENT, oldKeys));
        }
        primaryKeyLocalStore.put(new KeyValue(extraKey,
            Arrays.copyOf(oldKeyValue.getValue(), oldKeyValue.getValue().length))
        );
        oldKeyValue.setKey(deletePrimaryKey);
        primaryKeyLocalStore.put(oldKeyValue);
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        synchronized (vertex) {
            TxnPartReplaceIntoParam param = vertex.getParam();
            Edge edge = vertex.getSoleEdge();
            if (fin instanceof FinWithException) {
                edge.fin(fin);
                param.reset();
                return;
            }
            edge.transformToNext(new Object[]{param.getCount()});
            if (fin instanceof FinWithProfiles) {
                FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
                finWithProfiles.addProfile(vertex);
                Long autoIncId = !param.getAutoIncList().isEmpty() ? param.getAutoIncList().get(0) : null;
                if (autoIncId != null) {
                    finWithProfiles.getProfile().setAutoIncId(autoIncId);
                    finWithProfiles.getProfile().setHasAutoInc(true);
                    param.getAutoIncList().remove(0);
                }
            }
            edge.fin(fin);
            // Reset
            param.reset();
        }
    }
}
