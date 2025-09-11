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
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnPartUpdateParam;
import io.dingodb.exec.transaction.base.TxnPartData;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.tuple.TupleKey;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.exec.utils.OpStateUtils;
import io.dingodb.expr.rel.PipeOp;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.core.TableModify;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TxnPartUpdateOperator extends PartModifyOperator {
    public static final TxnPartUpdateOperator INSTANCE = new TxnPartUpdateOperator();

    private TxnPartUpdateOperator() {
    }

    @Override
    protected boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        TxnPartUpdateParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("partUpdate");
        long start = System.currentTimeMillis();
        param.setContext(context);
        DingoType schema = param.getSchema();
        TupleMapping mapping = param.getMapping();
        RelOp relOp = param.getRelOp();

        int tupleSize = schema.fieldCount();
        Object[] newTuple = Arrays.copyOf(tuple, tupleSize);
        Object[] copyTuple = Arrays.copyOf(tuple, tuple.length);
        boolean updated = false;
        int i;
        try {
            CommonId txnId = vertex.getTask().getTxnId();
            CommonId tableId = param.getTableId();
            List<Column> originColumns = new ArrayList<>();
            CommonId joinTableId = param.getJoinTableId();
            boolean isLeft = true;
            if (joinTableId != null) {
                if (context.getTableId() != null && !context.getTableId().equals(tableId)) {
                    tableId = context.getTableId();
                    joinTableId = param.getTableId();
                    isLeft = false;
                }
                List<Column> originCols = ((Table) TransactionManager.getTable(txnId, tableId)).getColumns();
                List<Column> joinColumns = ((Table) TransactionManager
                    .getTable(txnId, joinTableId)).getColumns();
                if (param.isLeft() && isLeft) {
                    originColumns.addAll(originCols);
                    originColumns.addAll(joinColumns);
                } else {
                    originColumns.addAll(joinColumns);
                    originColumns.addAll(originCols);
                }
            } else {
                originColumns = ((Table) TransactionManager.getTable(txnId, tableId)).getColumns();
            }

            KeyValueCodec codec = param.getCodec();
            for (i = 0; i < mapping.size(); ++i) {
                // Object newValue = updates.get(i).eval(tuple);
                Object newValue = ((Object[]) ((PipeOp) relOp).put(tuple))[i];
                int index = mapping.get(i);
                DingoType t = originColumns.get(index).getType();

                //Only for origin table.
                if (context.getIndexId() == null && !param.isPessimisticTxn()) {
                    if (t instanceof io.dingodb.common.type.scalar.DecimalType) {
                        if (newValue instanceof BigDecimal) {
                            long valueIntPart = ((BigDecimal) newValue).precision() - ((BigDecimal) newValue).scale();
                            long typeIntPart = ((io.dingodb.common.type.scalar.DecimalType) t).getPrecision()
                                - ((io.dingodb.common.type.scalar.DecimalType) t).getScale();
                            if (valueIntPart > typeIntPart) {
                                throw new DingoTypeRangeException(0, "Out of range value for column '" + originColumns.get(index).getName() + "'");
                            }
                        }
                    } else if(originColumns.get(index).getSqlTypeName().equalsIgnoreCase("TINYINT")) {
                        if(newValue instanceof Integer) {
                            if (!Utils.tinyintInRange((Integer) newValue)) {
                                throw new DingoTypeRangeException(0, "Out of range value for column '" + originColumns.get(index).getName() + "'");
                            }
                        }
                    }
                }

                if ((newTuple[index] == null && newValue != null)
                    || (newTuple[index] != null && !newTuple[index].equals(newValue))
                ) {
                    newTuple[index] = newValue;
                    updated = true;
                }
            }
            TableModify.TableInfo tableInfo = param.getTableInfo();
            if (!tableInfo.isSingleSource()) {
                // Multi-table update, extract the tuple of the current table
                MetaService metaService = MetaService.root();
                for (int i1 = 0; i1 < tableInfo.getTargetTableIndexes().size(); i1++) {
                    int tableIndex = tableInfo.getTargetTableIndexes().get(i1);
                    String tableName = tableInfo.getRefTableNames().get(tableIndex);
                    Table table = metaService.getTable(tableId);
                    if (table.getName().equals(tableName)) {
                        Object[] tableIndexes = tableInfo.getSourceColumnIndexMap().get(tableIndex)
                            .values().stream().sorted(Integer::compare).toArray(Object[]::new);
                        Object[] tuples = new Object[tableIndexes.length];
                        DingoType[] types = new DingoType[tableIndexes.length];
                        for (int j = 0; j < tableIndexes.length; j++) {
                            tuples[j] = newTuple[(Integer) tableIndexes[j]];
                            types[j] = originColumns.get((Integer) tableIndexes[j]).getType();
                        }
                        schema = DingoTypeFactory.tuple(types);
                        newTuple = (Object[]) schema.convertFrom(tuples, ValueConverter.INSTANCE);
                        codec = CodecService.getDefault().createKeyValueCodec(
                            table.getCodecVersion(), table.version, schema, table.keyMapping());
                    }
                }
            }
            if (param.isHasAutoInc() && param.getAutoIncColIdx() < tuple.length) {
                if (newTuple[param.getAutoIncColIdx()] != null) {
                    long autoIncVal = Long.parseLong(newTuple[param.getAutoIncColIdx()].toString());
                    MetaService metaService = MetaService.root();
                    metaService.updateAutoIncrement(tableId, autoIncVal);
                }
            }

            CommonId partId = context.getDistribution().getId();
            boolean calcPartId = false;
            boolean isVector = false;
            boolean isDocument = false;
            if (context.getIndexId() != null) {
                Table indexTable = (Table) TransactionManager.getIndex(txnId, context.getIndexId());
                if (indexTable == null) {
                    LogUtils.error(log, "[ddl] TxnPartUpdate get index table null, indexId:{}", context.getIndexId());
                    return false;
                }
                if (!OpStateUtils.allowWrite(indexTable.getSchemaState())) {
                    return true;
                }
                List<Integer> columnIndices = param.getTable().getColumnIndices(indexTable.columns.stream()
                    .map(Column::getName)
                    .collect(Collectors.toList()));
                Object defaultVal = null;
                if (columnIndices.contains(-1)) {
                    Column addColumn = indexTable.getColumns().stream()
                        .filter(column -> column.getSchemaState() != SchemaState.SCHEMA_PUBLIC)
                        .findFirst().orElse(null);
                    if (addColumn != null) {
                        defaultVal = addColumn.getDefaultVal();
                    }
                }
                tableId = context.getIndexId();
                schema = indexTable.tupleType();
                codec = CodecService.getDefault().createKeyValueCodec(
                    indexTable.getCodecVersion(), indexTable.version, indexTable.tupleType(), indexTable.keyMapping()
                );
                Object[] finalNewTuple = newTuple;
                Object finalDefaultVal = defaultVal;
                newTuple = columnIndices.stream().map(c -> {
                    if (c == -1) {
                        return finalDefaultVal;
                    }
                    return finalNewTuple[c];
                }).toArray();
                Object[] copyNewTuple = copyTuple;
                copyTuple = columnIndices.stream().map(c -> {
                    if (c == -1) {
                        return finalDefaultVal;
                    }
                    return copyNewTuple[c];
                }).toArray();
                if (updated && columnIndices.stream().anyMatch(mapping::contains)) {
                    PartitionService ps = PartitionService.getService(
                        Optional.ofNullable(indexTable.getPartitionStrategy())
                            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                    Object[] newTuple2 = (Object[]) schema.convertFrom(newTuple, ValueConverter.INSTANCE);
                    byte[] key = wrap(codec::encodeKey).apply(newTuple2);
                    partId = ps.calcPartId(key, MetaService.root().getRangeDistribution(tableId));
                    LogUtils.debug(log, "{} update index primary key is{} calcPartId is {}",
                        txnId,
                        Arrays.toString(key),
                        partId
                    );
                    calcPartId = true;
                }
                IndexTable index = (IndexTable) TransactionManager.getIndex(txnId, tableId);
                if (index.indexType.isVector) {
                    isVector = true;
                }
                if (index.indexType == IndexType.DOCUMENT) {
                    isDocument = true;
                }
            }
            Object[] newTuple2 = (Object[]) schema.convertFrom(newTuple, ValueConverter.INSTANCE);

            byte[] key = wrap(codec::encodeKey).apply(newTuple2);
            // new key need calcPartId
            if (updated && param.isUpdatePrimaryKey() && context.getIndexId() == null) {
                PartitionService ps = PartitionService.getService(
                    Optional.ofNullable(param.getTable().getPartitionStrategy())
                        .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                partId = ps.calcPartId(key, MetaService.root().getRangeDistribution(tableId));
                CodecService.getDefault().setId(key, partId.domain);
                LogUtils.debug(log, "{} update table primary key is{} calcPartId is {}",
                    txnId,
                    Arrays.toString(key),
                    partId
                );
                calcPartId = true;
            }
            CodecService.getDefault().setId(key, partId.domain);
            StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
            byte[] txnIdBytes = vertex.getTask().getTxnId().encode();
            byte[] tableIdBytes = tableId.encode();
            byte[] partIdBytes = partId.encode();
            byte[] jobIdByte = vertex.getTask().getJobId().encode();
            int len = txnIdBytes.length + tableIdBytes.length + partIdBytes.length;
            if (param.isPessimisticTxn()) {
                // dataKeyValue   [10_txnId_tableId_partId_a_putIf, value]
                byte[] dataKey = ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_DATA,
                    key,
                    Op.PUT.getCode(),
                    len,
                    txnIdBytes,
                    tableIdBytes,
                    partIdBytes
                );
                KeyValue oldKeyValue = localStore.get(dataKey);
                if (!updated) {
                    LogUtils.warn(log, "{} updated is false key is {}", txnId, Arrays.toString(key));
                    // data is not exist local store
                    if (oldKeyValue == null) {
                        Op op = Op.PUT;
                        if (context.getUpdateResidualDeleteKey().get()) {
                            op = Op.DELETE;
                        }
                        byte[] rollBackKey = ByteUtils.getKeyByOp(
                            CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, op, dataKey
                        );
                        KeyValue rollBackKeyValue = new KeyValue(rollBackKey, null);
                        LogUtils.debug(log, "{}, updated is false residual key is:{}",
                            txnId, Arrays.toString(rollBackKey));
                        vertex.getTask().getPartData().put(
                            new TxnPartData(tableId, partId),
                            (!isVector && !isDocument)
                        );
                        localStore.put(rollBackKeyValue);
                    }
                    return true;
                }
                byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
                deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
                byte[] insertKey = Arrays.copyOf(dataKey, dataKey.length);
                insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
                List<byte[]> bytes = new ArrayList<>(3);
                bytes.add(dataKey);
                bytes.add(deleteKey);
                bytes.add(insertKey);
                List<KeyValue> keyValues = localStore.get(bytes);
                if (keyValues != null && !keyValues.isEmpty()) {
                    if (keyValues.size() > 1) {
                        throw new RuntimeException(txnId + " PrimaryKey is not existed than two in local store");
                    }
                    LogUtils.debug(log, "{} updated is true, repeat key is {}", txnId, Arrays.toString(key));
                    KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                    CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                    // write data
                    keyValue.setKey(dataKey);
                    localStore.delete(deleteKey);
                    localStore.delete(insertKey);
                    if (updated) {
                        vertex.getTask().getPartData().put(
                            new TxnPartData(tableId, partId),
                            (!isVector && !isDocument)
                        );
                        localStore.delete(dataKey);
                        if (localStore.put(keyValue) && context.getIndexId() == null) {
                            param.inc();
                        }
                    }
                } else {
                    byte[] rollBackKey = ByteUtils.getKeyByOp(
                        CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey
                    );
                    // first lock and kvGet is null
                    if (localStore.get(rollBackKey) != null) {
                        vertex.getTask().getPartData().put(
                            new TxnPartData(tableId, partId),
                            (!isVector && !isDocument)
                        );
                        return true;
                    } else {
                        rollBackKey = ByteUtils.getKeyByOp(
                            CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.PUT, dataKey
                        );
                        if (localStore.get(rollBackKey) != null) {
                            localStore.delete(rollBackKey);
                        }
                        KeyValue kv = wrap(codec::encode).apply(newTuple2);
                        CodecService.getDefault().setId(kv.getKey(), partId.domain);
                        // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
                        byte[] extraKey = ByteUtils.encode(
                            CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                            kv.getKey(),
                            Op.NONE.getCode(),
                            len,
                            jobIdByte,
                            tableIdBytes,
                            partIdBytes
                        );
                        localStore.put(new KeyValue(extraKey, Arrays.copyOf(kv.getValue(), kv.getValue().length)));
                        // write data
                        kv.setKey(dataKey);
                        vertex.getTask().getPartData().put(
                            new TxnPartData(tableId, partId),
                            (!isVector && !isDocument)
                        );
                        if (localStore.put(kv)
                            && context.getIndexId() == null
                        ) {
                            param.inc();
                        }
                    }
                }
                boolean isUpdateMainTablePrimaryKey = (param.isUpdatePrimaryKey() && context.getIndexId() == null && updated);
                if (calcPartId && isUpdateMainTablePrimaryKey) {
                    // delete old key
                    oldKeyValue = wrap(codec::encode).apply(Arrays.copyOf(copyTuple, schema.fieldCount()));
                    byte[] oldKey = oldKeyValue.getKey();
                    partId = context.getDistribution().getId();
                    CodecService.getDefault().setId(oldKey, partId.domain);
                    localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
                    partIdBytes = partId.encode();
                    dataKey = ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_DATA,
                        oldKey,
                    Op.PUTIFABSENT.getCode(),
                    len,
                    txnIdBytes,
                    tableIdBytes,
                    partIdBytes);
                    localStore.delete(dataKey);
                    byte[] updateKey = Arrays.copyOf(dataKey, dataKey.length);
                    updateKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
                    localStore.delete(updateKey);
                    deleteKey = Arrays.copyOf(dataKey, dataKey.length);
                    deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
                    vertex.getTask().getPartData().put(
                        new TxnPartData(tableId, partId),
                        (!isVector && !isDocument)
                    );
                    localStore.put(new KeyValue(deleteKey, Arrays.copyOf(oldKeyValue.getValue(), oldKeyValue.getValue().length)));
                }
            } else {
                if (param.getUpdateLimit() != -1L || !param.getTableInfo().isSingleSource()) {
                    long scanCount = param.getUpdateScanCount();
                    long limit = param.getUpdateLimit();

                    if (scanCount >= limit) {
                        if (scanCount == limit && param.getIndexSize() > 0) {
                            TupleKey tupleKey = new TupleKey(tuple);
                            if (param.getUpdateKeys().containsKey(tupleKey)) {
                                param.getUpdateKeys().computeIfPresent(tupleKey, (k, v) -> v + 1);
                            } else {
                                return false;
                            }
                        } else {
                            if (!param.getTableInfo().isSingleSource()) {
                                TupleKey tupleKey = new TupleKey(newTuple);
                                if (!param.getUpdateKeys().containsKey(tupleKey)) {
                                    param.getUpdateKeys().putIfAbsent(tupleKey, 0);
                                } else {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
                    } else {
                        if (context.getIndexId() == null) {
                            param.incUpdateScanCount();
                            param.getUpdateKeys().putIfAbsent(new TupleKey(Arrays.copyOf(tuple, tuple.length)), 0);
                        } else {
                            TupleKey tupleKey = new TupleKey(tuple);
                            if (param.getUpdateKeys().containsKey(tupleKey)) {
                                param.getUpdateKeys().computeIfPresent(tupleKey, (k, v) -> v + 1);
                            } else {
                                return false;
                            }
                        }
                    }
                }

                KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                LogUtils.debug(log, "{} update key is {}, partId is {}",
                    txnId, Arrays.toString(keyValue.getKey()), partId);
                if (calcPartId) {
                    // begin insert update commit
                    KeyValue oldKeyValue = wrap(codec::encode).apply(
                        param.isUpdatePrimaryKey() ? Arrays.copyOf(copyTuple, schema.fieldCount()) : copyTuple
                    );
                    byte[] oldKey = oldKeyValue.getKey();
                    CodecService.getDefault().setId(oldKey, context.getDistribution().getId().domain);
                    boolean isMainTable = (context.getIndexId() == null && param.isUpdatePrimaryKey());
                    if (!ByteArrayUtils.equal(keyValue.getKey(), oldKey)) {
                        // look up new key and delete old key
                        // loop up new key
                        {
                            localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
                            byte[] keyValueKey = keyValue.getKey();
                            byte[] insertKey = ByteUtils.encode(
                                CommonId.CommonType.TXN_CACHE_DATA,
                                keyValueKey,
                                Op.PUTIFABSENT.getCode(),
                                len,
                                txnIdBytes,
                                tableIdBytes,
                                partIdBytes);
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
                            if (keyValues != null && !keyValues.isEmpty()) {
                                if (keyValues.size() > 1) {
                                    throw new RuntimeException(txnId + " Key is not existed than two in local store");
                                }
                                KeyValue value = keyValues.get(0);
                                byte[] newKey = value.getKey();
                                if (newKey[newKey.length - 2] == Op.PUTIFABSENT.getCode()
                                    || newKey[newKey.length - 2] == Op.PUT.getCode()) {
                                    throw new DuplicateEntryException("Duplicate entry "
                                        + TransactionUtil.duplicateEntryKey(tableId, key, txnId) +
                                        " for key 'PRIMARY'");
                                } else {
                                    // delete  ->  insert  convert --> put
                                    insertKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
                                    op = Op.DELETE;
                                }
                            } else {
                                if (isMainTable) {
                                    StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
                                    KeyValue kvKeyValue = kvStore.txnGet(
                                        TsoService.getDefault().tso(),
                                        keyValueKey,
                                        param.getLockTimeOut()
                                    );
                                    if (kvKeyValue != null && kvKeyValue.getValue() != null) {
                                        throw new DuplicateEntryException("Duplicate entry " +
                                            TransactionUtil.duplicateEntryKey(tableId, keyValueKey, txnId) +
                                            " for key 'PRIMARY'");
                                    }
                                }
                                keyValue.setKey(
                                    ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_CHECK_DATA,
                                        Op.CheckNotExists, insertKey)
                                );
                                localStore.put(keyValue);
                            }
                            keyValue.setKey(
                                insertKey
                            );
                            localStore.delete(deleteKey);
                            // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
                            byte[] extraKey = ByteUtils.encode(
                                CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                                key,
                                op.getCode(),
                                len,
                                jobIdByte,
                                tableIdBytes,
                                partIdBytes
                            );
                            vertex.getTask().getPartData().put(
                                new TxnPartData(tableId, partId),
                                (!isVector && !isDocument)
                            );
                            localStore.put(
                                new KeyValue(extraKey, Arrays.copyOf(keyValue.getValue(), keyValue.getValue().length))
                            );
                            localStore.put(keyValue);
                        }
                        // delete old key
                        {
                            localStore = Services.LOCAL_STORE.getInstance(tableId, context.getDistribution().getId());
                            byte[] oldDataKey = ByteUtils.encode(
                                CommonId.CommonType.TXN_CACHE_DATA,
                                oldKey,
                                Op.PUT.getCode(),
                                len,
                                txnIdBytes,
                                tableIdBytes,
                                context.getDistribution().getId().encode());
                            Op op = Op.NONE;
                            if (localStore.get(oldDataKey) != null) {
                                op = Op.PUT;
                            }
                            localStore.delete(oldDataKey);
                            byte[] updateKey = Arrays.copyOf(oldDataKey, oldDataKey.length);
                            updateKey[updateKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
                            if (localStore.get(updateKey) != null) {
                                op = Op.PUTIFABSENT;
                            }
                            localStore.delete(updateKey);
                            byte[] deleteKey = Arrays.copyOf(oldDataKey, oldDataKey.length);
                            deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
                            // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
                            byte[] extraKey = ByteUtils.encode(
                                CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                                oldKey,
                                op.getCode(),
                                len,
                                jobIdByte,
                                tableIdBytes,
                                context.getDistribution().getId().encode()
                            );
                            vertex.getTask().getPartData().put(
                                new TxnPartData(tableId, context.getDistribution().getId()),
                                (!isVector && !isDocument)
                            );
                            localStore.put(new KeyValue(extraKey, oldKeyValue.getValue()));
                            localStore.put(new KeyValue(deleteKey, oldKeyValue.getValue()));
                        }
                        if (context.getIndexId() == null) {
                            param.inc();
                            context.addKeyState(true);
                        }
                        profile.time(start);
                        return true;
                    }
                }
                if (updated) {
                    byte[] keyValueKey = keyValue.getKey();
                    keyValue.setKey(
                        ByteUtils.encode(
                            CommonId.CommonType.TXN_CACHE_DATA,
                            keyValueKey,
                            Op.PUT.getCode(),
                            len,
                            txnIdBytes,
                            tableIdBytes,
                            partIdBytes)
                    );
                    Op op = Op.NONE;
                    byte[] insertKey = Arrays.copyOf(keyValue.getKey(), keyValue.getKey().length);
                    insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
                    if (localStore.get(insertKey) != null) {
                        op = Op.PUTIFABSENT;
                    }
                    localStore.delete(insertKey);
                    // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
                    byte[] extraKey = ByteUtils.encode(
                        CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                        keyValueKey,
                        op.getCode(),
                        len,
                        jobIdByte,
                        tableIdBytes,
                        partIdBytes
                    );
                    localStore.put(
                        new KeyValue(extraKey, Arrays.copyOf(keyValue.getValue(), keyValue.getValue().length))
                    );
                    localStore.delete(keyValue.getKey());
                    vertex.getTask().getPartData().put(
                        new TxnPartData(tableId, partId),
                        (!isVector && !isDocument)
                    );
                    if (localStore.put(keyValue) && context.getIndexId() == null) {
                        param.inc();
                        context.addKeyState(true);
                    }
                }
            }
        } catch(DingoTypeRangeException ex) {
            throw ex;
        } catch (Exception ex) {
            LogUtils.error(log, ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
        profile.time(start);
        return true;
    }

}
