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
import io.dingodb.common.type.NullableType;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnPartInsertIgnoreParam;
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
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.utils.ColumnDefaultValueUtils.getDefaultValue;

@Slf4j
public class TxnPartInsertIgnoreOperator extends PartModifyOperator {
    public static final TxnPartInsertIgnoreOperator INSTANCE = new TxnPartInsertIgnoreOperator();

    private TxnPartInsertIgnoreOperator() {
    }

    @Override
    protected boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        TxnPartInsertIgnoreParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("partInsertIgnoreLocal");
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
        Table indexTable = null;

        Utils.checkAndUpdateTuples(schema, tuple);

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
                } else if(originColumns.get(i).getSqlTypeName().equalsIgnoreCase("TINYINT")) {
                    if(tuple[i] instanceof Integer) {
                        if (!Utils.tinyintInRange((Integer)tuple[i])) {
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
            if (!param.isPessimisticTxn()) {
            }
            Object defaultVal = null;
            if (columnIndices.contains(-1)) {
                Column addColumn = indexTable.getColumns().stream()
                    .filter(column -> column.getSchemaState() != SchemaState.SCHEMA_PUBLIC)
                    .findFirst().orElse(null);
                if (addColumn != null) {
                    defaultVal = addColumn.getDefaultVal();
                }
            }
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
            schema = indexTable.tupleType();
            localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
            codec = CodecService.getDefault().createKeyValueCodec(
                indexTable.getCodecVersion(), indexTable.version, indexTable.tupleType(), indexTable.keyMapping()
            );
        }
        if (context.isWithoutPrimary()) {
            schema.setCheckFieldCount(false);
            DingoType dingoType = codec.getDingoType();
            if (dingoType != null) {
                dingoType.setCheckFieldCount(false);
            }
        }
        Object[] newTuple = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
        assert newTuple != null;
        KeyValue keyValue = wrap(codec::encode).apply(newTuple);
        boolean containsNull = tuple != null && Arrays.stream(tuple).anyMatch(Objects::isNull);
        if (containsNull) {
            DingoType[] fields = ((TupleType) schema).getFields();
            for (int i = 0; i < tuple.length; i++) {
                DingoType type = fields[i];
                if(tuple[i] == null && !((NullableType)type).isNullable()) {
                    newTuple[i] = getDefaultValue(type);
                }
            }
            keyValue = wrap(codec::encode).apply(newTuple);
        }
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
                if (oldKey[oldKey.length - 2] == Op.PUTIFABSENT.getCode()
                    || oldKey[oldKey.length - 2] == Op.PUT.getCode()) {
                    profile.time(start - System.currentTimeMillis());
                    return true;
                } else {
                    // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
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
                    // delete  ->  insert  convert --> put
                    dataKey[dataKey.length - 2] = (byte) Op.PUT.getCode();
                    // write data
                    keyValue.setKey(dataKey);
                    localStore.delete(deleteKey);
                    vertex.getTask().getPartData().put(
                        new TxnPartData(tableId, partId),
                        (!isVector && !isDocument)
                    );
                    if (localStore.put(keyValue) && context.getIndexId() == null) {
                        param.inc();
                    }
                }
            } else {
                byte[] rollBackKey = ByteUtils.getKeyByOp(
                    CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey
                );
                if (localStore.get(rollBackKey) != null) {
                    profile.time(start - System.currentTimeMillis());
                    return true;
                } else {
                    if (context.getIndexId() == null) {
                        for (IndexTable currentIndex : param.getTable().getIndexes()) {
                            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution =
                                MetaService.root().getRangeDistribution(currentIndex.tableId);
                            if (!currentIndex.unique) {
                                continue;
                            }
                            KeyValue uniqueIndexKeyValue = getUniqueIndexKeyValue(
                                tuple,
                                param,
                                txnId,
                                txnIdByte,
                                currentIndex,
                                rangeDistribution
                            );
                            if (uniqueIndexKeyValue != null && uniqueIndexKeyValue.getValue() != null) {
                                profile.time(start - System.currentTimeMillis());
                                return true;
                            }
                        }
                    }
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
                keyValue.setKey(dataKey);
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
                    profile.time(start - System.currentTimeMillis());
                    return true;
                } else {
                    // delete  ->  insert  convert --> put
                    insertKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
                    op = Op.DELETE;
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
                KeyValue kvKeyValue = kvStore.txnGet(
                    txnId.seq,
                    originalKey,
                    param.getLockTimeOut()
                );
                if (kvKeyValue != null && kvKeyValue.getValue() != null) {
                    profile.time(start - System.currentTimeMillis());
                    return true;
                } else {
                    if (context.getIndexId() == null) {
                        for (IndexTable currentIndex : param.getTable().getIndexes()) {
                            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution =
                                MetaService.root().getRangeDistribution(currentIndex.tableId);
                            if (!currentIndex.unique) {
                                continue;
                            }
                            KeyValue uniqueIndexKeyValue = getUniqueIndexKeyValue(
                                tuple,
                                param,
                                txnId,
                                txnIdByte,
                                currentIndex,
                                rangeDistribution
                            );
                            if (uniqueIndexKeyValue != null && uniqueIndexKeyValue.getValue() != null) {
                                profile.time(start - System.currentTimeMillis());
                                return true;
                            }
                        }
                    }
                }
                keyValue.setKey(
                    ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_CHECK_DATA, Op.CheckNotExists, insertKey)
                );
                localStore.put(keyValue);
            }
            keyValue.setKey(insertKey);
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


    private static KeyValue getUniqueIndexKeyValue(Object[] tuple,
                                        TxnPartInsertIgnoreParam param,
                                        CommonId txnId,
                                        byte[] txnIdByte,
                                        IndexTable currentIndex,
                                        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution) {
        byte[] indexRegionIdByte;
        List<Integer> columnIndices = param.getTable().getColumnIndices(currentIndex.columns.stream()
            .map(Column::getName)
            .collect(Collectors.toList()));
        CommonId indexTableId = currentIndex.tableId;
        byte[] indexTableIdByte = indexTableId.encode();
        Object defaultVal = null;
        if (columnIndices.contains(-1)) {
            Column addColumn = currentIndex.getColumns().stream()
                .filter(column -> column.getSchemaState() != SchemaState.SCHEMA_PUBLIC)
                .findFirst().orElse(null);
            if (addColumn != null) {
                defaultVal = addColumn.getDefaultVal();
            }
        }
        Object[] finalTuple = Arrays.copyOf(tuple, tuple.length);
        Object finalDefaultVal = defaultVal;
        Object[] oldIndexTuple = columnIndices.stream().map(i -> {
            if (i == -1) {
                return finalDefaultVal;
            }
            return finalTuple[i];
        }).toArray();
        // get index data
        KeyValueCodec indexCodec = CodecService.getDefault().createKeyValueCodec(
            currentIndex.getCodecVersion(), currentIndex.version, currentIndex.tupleType(), currentIndex.keyMapping()
        );
        KeyValue oldKeyValue = indexCodec.encode(oldIndexTuple);
        CommonId indexRegionId = PartitionService.getService(
                Optional.ofNullable(currentIndex.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(oldKeyValue.getKey(), rangeDistribution);
        indexRegionIdByte = indexRegionId.encode();
        byte[] oldKeys = CodecService.getDefault().setId(oldKeyValue.getKey(), indexRegionId.domain);
            StoreInstance indexKvStore = Services.KV_STORE.getInstance(indexTableId, indexRegionId);
        KeyValue primaryKeyValue = indexKvStore.txnGet(
                txnId.seq,
                oldKeys,
                param.getLockTimeOut()
            );
        if (primaryKeyValue != null && primaryKeyValue.getValue() != null) {
            return primaryKeyValue;
        }
        byte[] primaryKey = ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_DATA,
            oldKeys,
            Op.PUTIFABSENT.getCode(),
            (txnIdByte.length + indexTableIdByte.length + indexRegionIdByte.length),
            txnIdByte,
            indexTableIdByte,
            indexRegionIdByte
        );
        StoreInstance primaryKeyLocalStore = Services.LOCAL_STORE.getInstance(indexTableId, indexRegionId);
        primaryKeyValue = primaryKeyLocalStore.get(primaryKey);
        if (primaryKeyValue != null && primaryKeyValue.getValue() != null) {
            return primaryKeyValue;
        }
        return primaryKeyLocalStore.get(ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_DATA, Op.PUT, primaryKey));
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        synchronized (vertex) {
            TxnPartInsertIgnoreParam param = vertex.getParam();
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
