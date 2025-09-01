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
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.exception.DingoTypeRangeException;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.NullableType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Status;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.exception.TaskCancelException;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PessimisticLockParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
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
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.utils.ByteUtils.encode;
import static io.dingodb.exec.utils.ByteUtils.getKeyByOp;
import static io.dingodb.exec.utils.ColumnDefaultValueUtils.getDefaultValue;

@Slf4j
public class PessimisticLockOperator extends SoleOutOperator {
    public static final PessimisticLockOperator INSTANCE = new PessimisticLockOperator();

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            PessimisticLockParam param = vertex.getParam();
            param.setContext(context);
            CommonId txnId = vertex.getTask().getTxnId();
            CommonId tableId = param.getTableId();
            CommonId partId = context.getDistribution().getId();
            CommonId jobId = vertex.getTask().getJobId();
            byte[] primaryLockKey = param.getPrimaryLockKey();
            ITransaction transaction = TransactionManager.getTransaction(txnId);
            if (transaction == null || (primaryLockKey == null && transaction.getPrimaryKeyLock() != null)) {
                return false;
            }
            if (param.getUpdateLimit() != -1L) {
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
                        return false;
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
            DingoType schema = param.getSchema();
            StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
            KeyValueCodec codec = param.getCodec();
            boolean updated = false;
            int tupleSize = schema.fieldCount();
            Object[] oldTuple = Arrays.copyOf(tuple, tupleSize);
            boolean isVector = false;
            boolean isDocument = false;

            //Only for origin table.
            if (context.getIndexId() == null) {
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
                            if ( (Integer)tuple[i] >= 127 || (Integer)tuple[i] <= -128) {
                                throw new DingoTypeRangeException(0, "Out of range value for column '" + originColumns.get(i).getName() + "'");
                            }
                        }
                    }
                }
            }

            if (context.getIndexId() != null) {
                if (primaryLockKey == null) {
                    return true;
                }
                IndexTable indexTable = (IndexTable) TransactionManager.getIndex(txnId, context.getIndexId());
                if (indexTable == null) {
                    LogUtils.error(log, "Pessimistic lock get index table null, indexId:{}", context.getIndexId());
                    return false;
                }
                if (!OpStateUtils.allowOpContinue(param.getOpType(), indexTable.schemaState)) {
                    return false;
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
                Object finalDefaultVal = defaultVal;
                tableId = context.getIndexId();
                Object[] finalTuple = tuple;
                tuple = columnIndices.stream().map(i -> {
                    if (i == -1) {
                        return finalDefaultVal;
                    }
                    return finalTuple[i];
                }).toArray();
                schema = indexTable.tupleType();
                if (indexTable.indexType.isVector) {
                    isVector = true;
                }
                if (indexTable.indexType == IndexType.DOCUMENT) {
                    isDocument = true;
                }
                localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
                codec = CodecService.getDefault().createKeyValueCodec(indexTable.getCodecVersion(), indexTable.version,
                    indexTable.tupleType(), indexTable.keyMapping());
            }
            StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
            Object[] newTuple = Arrays.copyOf(tuple, tupleSize);
            byte[] key;
            // isUpdatePrimaryKey look up new key and delete old key
            if (param.isUpdatePrimaryKey() && context.getIndexId() == null) {
                TupleMapping mapping = param.getMapping();
                List<SqlExpr> updates = param.getUpdates();
                RelOp relOp = param.getRelOp();
                for (int i = 0; i < mapping.size(); ++i) {
                    Object newValue;
                    if (relOp != null) {
                        newValue = ((Object[]) ((PipeOp) relOp).put(tuple))[i];
                    } else {
                        newValue = updates.get(i).eval(tuple);
                    }
                    int index = mapping.get(i);
                    if ((newTuple[index] == null && newValue != null)
                        || (newTuple[index] != null && !newTuple[index].equals(newValue))
                    ) {
                        newTuple[index] = newValue;
                        updated = true;
                    }
                }
                if (updated) {
                    PartitionService ps = PartitionService.getService(
                        Optional.ofNullable(param.getTable().getPartitionStrategy())
                            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                    // new key
                    key = wrap(codec::encodeKey).apply(newTuple);
                    partId = ps.calcPartId(key, MetaService.root().getRangeDistribution(tableId));
                    LogUtils.debug(log, "{} update table primary key is{} calcPartId is {}",
                        txnId,
                        Arrays.toString(key),
                        partId
                    );
                } else {
                    key = wrap(codec::encodeKey).apply(newTuple);
                }
            } else {
                if (schema.fieldCount() != tuple.length) {
                    Object[] dest = new Object[schema.fieldCount()];
                    System.arraycopy(tuple, 0, dest, 0, schema.fieldCount());
                    newTuple = (Object[]) schema.convertFrom(dest, ValueConverter.INSTANCE);
                } else {
                    newTuple = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
                }
                key = wrap(codec::encodeKey).apply(newTuple);
            }
            if (param.isIgnore()) {
                boolean containsNull = newTuple != null && Arrays.stream(newTuple).anyMatch(Objects::isNull);
                if (containsNull) {
                    DingoType[] fields = ((TupleType) schema).getFields();
                    for (int i = 0; i < newTuple.length; i++) {
                        DingoType type = fields[i];
                        if(newTuple[i] == null && !((NullableType)type).isNullable()) {
                            newTuple[i] = getDefaultValue(type);
                        }
                    }
                    key = wrap(codec::encodeKey).apply(newTuple);
                }
            }
            CodecService.getDefault().setId(key, partId.domain);
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
            byte[] txnIdByte = txnId.encode();
            byte[] tableIdByte = tableId.encode();
            byte[] partIdByte = partId.encode();
            byte[] jobIdByte = vertex.getTask().getJobId().encode();
            int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
            byte[] lockKeyBytes = encode(
                CommonId.CommonType.TXN_CACHE_LOCK,
                key,
                Op.LOCK.getCode(),
                len,
                txnIdByte,
                tableIdByte,
                partIdByte
            );
            KeyValue oldKeyValue = localStore.get(lockKeyBytes);
            if (oldKeyValue == null) {
                // for check deadLock
                byte[] deadLockKeyBytes = encode(
                    CommonId.CommonType.TXN_CACHE_BLOCK_LOCK,
                    key,
                    Op.LOCK.getCode(),
                    len,
                    txnIdByte,
                    tableIdByte,
                    partIdByte
                );
                KeyValue deadLockKeyValue = new KeyValue(deadLockKeyBytes, null);
                localStore.put(deadLockKeyValue);
                // first
                // add
                byte[] primaryKey = Arrays.copyOf(key, key.length);
                long startTs = param.getStartTs();
                LogUtils.debug(log, "{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, jobId.seq, Arrays.toString(primaryKey));
                Future future = null;
                List<KeyValue> kvKeyValue = new ArrayList<KeyValue>();

                TxnPessimisticLock txnPessimisticLock = TxnPessimisticLock.builder().
                    isolationLevel(IsolationLevel.of(param.getIsolationLevel()))
                    .primaryLock(primaryKey)
                    .mutations(Collections.singletonList(
                        TransactionCacheToMutation.cacheToPessimisticLockMutation(
                            primaryKey, TransactionUtil.toLockExtraData(
                                tableId,
                                partId,
                                txnId,
                                TransactionType.PESSIMISTIC.getCode()
                            ), jobId.seq
                        )
                    ))
                    .lockTtl(TransactionManager.lockTtlTm())
                    .startTs(startTs)
                    .forUpdateTs(jobId.seq)
                    .returnValues(true)
                    .build();
                try {
                    future = kvStore.txnPessimisticLockPrimaryKey(txnPessimisticLock, param.getLockTimeOut(), param.isScan(), kvKeyValue);
                } catch (RegionSplitException e) {
                    LogUtils.error(log, e.getMessage(), e);
                    CommonId regionId = TransactionUtil.singleKeySplitRegionId(tableId, txnId, primaryKey);
                    kvStore = Services.KV_STORE.getInstance(tableId, regionId);
                    future = kvStore.txnPessimisticLockPrimaryKey(txnPessimisticLock, param.getLockTimeOut(), param.isScan(), kvKeyValue);
                } catch (Throwable e) {
                    LogUtils.error(log, e.getMessage(), e);
                    TransactionUtil.resolvePessimisticLock(
                        param.getIsolationLevel(),
                        txnId,
                        tableId,
                        partId,
                        deadLockKeyBytes,
                        primaryKey,
                        startTs,
                        txnPessimisticLock.getForUpdateTs(),
                        true,
                        e
                    );
                }
                if (future == null) {
                    TransactionUtil.resolvePessimisticLock(
                        param.getIsolationLevel(),
                        txnId,
                        tableId,
                        partId,
                        deadLockKeyBytes,
                        primaryKey,
                        startTs,
                        txnPessimisticLock.getForUpdateTs(),
                        true,
                        new RuntimeException(txnId + " future is null " + partId + ",txnPessimisticLockPrimaryKey false")
                    );
                }
                boolean isUpdateMainTablePrimaryKey = (param.isUpdatePrimaryKey() && context.getIndexId() == null && updated);
                if (param.isInsert() || isUpdateMainTablePrimaryKey) {
                    if (kvKeyValue.size() != 0 && kvKeyValue.get(0) != null && kvKeyValue.get(0).getValue() != null) {
                        if (!param.isDuplicateUpdate() && !param.isReplaceInto() && !param.isIgnore()) {
                            if (future != null) {
                                future.cancel(true);
                            }
                            TransactionUtil.resolvePessimisticLock(
                                param.getIsolationLevel(),
                                txnId,
                                tableId,
                                partId,
                                deadLockKeyBytes,
                                primaryKey,
                                startTs,
                                txnPessimisticLock.getForUpdateTs(),
                                true,
                                new DuplicateEntryException("Duplicate entry " +
                                    TransactionUtil.duplicateEntryKey(CommonId.decode(tableIdByte), primaryKey, txnId) + " for key 'PRIMARY'")
                            );
                        }
                    }
                }
                long forUpdateTs = txnPessimisticLock.getForUpdateTs();
                LogUtils.debug(log, "{}, forUpdateTs:{} txnPessimisticLock :{} end", txnId, forUpdateTs, Arrays.toString(primaryKey));
                // get lock success, delete deadLockKey
                localStore.delete(deadLockKeyBytes);
                // lockKeyValue  [11_txnId_tableId_partId_a_lock, forUpdateTs1]
                byte[] primaryKeyLock = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                transaction.setPrimaryKeyLock(primaryKeyLock);
                if (!Arrays.equals(transaction.getPrimaryKeyLock(), primaryKeyLock)) {
                    if (future != null) {
                        future.cancel(true);
                    }
                    TransactionUtil.resolvePessimisticLock(
                        param.getIsolationLevel(),
                        txnId,
                        tableId,
                        partId,
                        deadLockKeyBytes,
                        primaryKey,
                        startTs,
                        txnPessimisticLock.getForUpdateTs(),
                        false,
                        null
                    );
                    return false;
                }
                transaction.setForUpdateTs(forUpdateTs);
                transaction.setPrimaryKeyFuture(future);
                if (kvKeyValue.size() != 0 && kvKeyValue.get(0) != null && kvKeyValue.get(0).getValue() != null) {
                    // extraKeyValue
                    KeyValue extraKeyValue = new KeyValue(
                        ByteUtils.encode(
                            CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                            key,
                            Op.NONE.getCode(),
                            len,
                            jobIdByte,
                            tableIdByte,
                            partIdByte),
                        kvKeyValue.get(0).getValue()
                    );
                    LogUtils.info(log, "PessimisticLock jobId:{}", CommonId.decode(jobIdByte));
                    localStore.put(extraKeyValue);
                    if (param.isIgnore()) {
                        LogUtils.info(log, "PessimisticLock RESIDUAL_LOCK jobId:{}", CommonId.decode(jobIdByte));
                        byte[] rollBackKey = ByteUtils.getKeyByOp(
                            CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK,
                            Op.DELETE,
                            deadLockKeyBytes
                        );
                        localStore.put(new KeyValue(rollBackKey, kvKeyValue.get(0).getValue()));
                    }
                } else {
                    if (param.isInsert()) {
                        KeyValue keyValue = wrap(codec::encode).apply(newTuple);
                        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                        // extraKeyValue
                        KeyValue extraKeyValue = new KeyValue(
                            ByteUtils.encode(
                                CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                                key,
                                Op.NONE.getCode(),
                                len,
                                jobIdByte,
                                tableIdByte,
                                partIdByte),
                            keyValue.getValue()
                        );
                        localStore.put(extraKeyValue);
                    } else {
                        if (isUpdateMainTablePrimaryKey) {
                            KeyValue keyValue = wrap(codec::encode).apply(newTuple);
                            CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                            // extraKeyValue
                            KeyValue extraKeyValue = new KeyValue(
                                ByteUtils.encode(
                                    CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                                    key,
                                    Op.NONE.getCode(),
                                    len,
                                    jobIdByte,
                                    tableIdByte,
                                    partIdByte),
                                keyValue.getValue()
                            );
                            localStore.put(extraKeyValue);
                        } else {
                            LogUtils.info(log, "PessimisticLock RESIDUAL_LOCK jobId:{}", CommonId.decode(jobIdByte));
                            byte[] rollBackKey = ByteUtils.getKeyByOp(
                                CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK,
                                Op.DELETE,
                                deadLockKeyBytes
                            );
                            localStore.put(new KeyValue(rollBackKey, null));
                        }
                    }
                }
                if (param.isForUpdate()) {
                    byte[] rollBackKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.LOCK, deadLockKeyBytes);
                    localStore.put(new KeyValue(rollBackKey, null));
                }
                byte[] lockKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                // lockKeyValue
                KeyValue lockKeyValue = new KeyValue(lockKey, PrimitiveCodec.encodeLong(forUpdateTs));
                localStore.put(lockKeyValue);
                {
                    // old key need lock and delete
                    if (isUpdateMainTablePrimaryKey) {
                        partId = context.getDistribution().getId();
                        localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
                        // delete old key
                        KeyValue keyValue = wrap(codec::encode).apply(oldTuple);
                        key = keyValue.getKey();
                        CodecService.getDefault().setId(key, partId.domain);
                        partIdByte = partId.encode();
                        // for check deadLock
                        deadLockKeyBytes = encode(
                            CommonId.CommonType.TXN_CACHE_BLOCK_LOCK,
                            key,
                            Op.LOCK.getCode(),
                            len,
                            txnIdByte,
                            tableIdByte,
                            partIdByte
                        );
                        deadLockKeyValue = new KeyValue(deadLockKeyBytes, null);
                        localStore.put(deadLockKeyValue);
                        LogUtils.debug(log, "{}, forUpdateTs:{} txnPessimisticLock :{}",
                            txnId, forUpdateTs, Arrays.toString(key));
                        if (vertex.getTask().getStatus() == Status.STOPPED) {
                            LogUtils.warn(log, "Task status is stop...");
                            // delete deadLockKey
                            localStore.delete(deadLockKeyBytes);
                            return false;
                        } else if (vertex.getTask().getStatus() == Status.CANCEL) {
                            LogUtils.warn(log, "Task status is cancel...");
                            // delete deadLockKey
                            localStore.delete(deadLockKeyBytes);
                            throw new TaskCancelException("task is cancel");
                        }
                        txnPessimisticLock = TransactionUtil.getTxnPessimisticLock(
                            txnId,
                            tableId,
                            partId,
                            primaryKey,
                            key,
                            param.getStartTs(),
                            forUpdateTs,
                            param.getIsolationLevel(),
                            true
                        );
                        try {
                            keyValue = TransactionUtil.pessimisticLock(
                                txnPessimisticLock,
                                param.getLockTimeOut(),
                                txnId,
                                tableId,
                                partId,
                                key,
                                param.isScan()
                            );
                            long newForUpdateTs = txnPessimisticLock.getForUpdateTs();
                            if (newForUpdateTs != forUpdateTs) {
                                forUpdateTs = newForUpdateTs;
                            }
                            LogUtils.debug(log, "{}, forUpdateTs:{} txnPessimisticLock :{}",
                                txnId, newForUpdateTs, Arrays.toString(key));
                            if (vertex.getTask().getStatus() == Status.STOPPED) {
                                TransactionUtil.resolvePessimisticLock(
                                    param.getIsolationLevel(),
                                    txnId,
                                    tableId,
                                    partId,
                                    deadLockKeyBytes,
                                    key,
                                    param.getStartTs(),
                                    txnPessimisticLock.getForUpdateTs(),
                                    false,
                                    null
                                );
                                return false;
                            } else if (vertex.getTask().getStatus() == Status.CANCEL) {
                                throw new TaskCancelException("task is cancel");
                            }
                        } catch (Throwable throwable) {
                            LogUtils.error(log, throwable.getMessage(), throwable);
                            TransactionUtil.resolvePessimisticLock(
                                param.getIsolationLevel(),
                                txnId,
                                tableId,
                                partId,
                                deadLockKeyBytes,
                                key,
                                param.getStartTs(),
                                txnPessimisticLock.getForUpdateTs(),
                                true,
                                throwable
                            );
                        }
                        // get lock success, delete deadLockKey
                        localStore.delete(deadLockKeyBytes);
                        lockKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                        // lockKeyValue
                        lockKeyValue = new KeyValue(lockKey, PrimitiveCodec.encodeLong(forUpdateTs));
                        localStore.put(lockKeyValue);
                        // extraKeyValue
                        KeyValue extraKeyValue = new KeyValue(
                            ByteUtils.encode(
                                CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                                key,
                                Op.NONE.getCode(),
                                len,
                                jobIdByte,
                                tableIdByte,
                                partIdByte),
                            keyValue.getValue()
                        );
                        localStore.put(extraKeyValue);
                    }
                }
                return false;
            } else {
                LogUtils.warn(log, "{}, key exist in localStore :{} ", txnId, Arrays.toString(key));
            }
            return true;
        }
    }


    @Override
    public synchronized void fin(int pin, Fin fin, Vertex vertex) {
        PessimisticLockParam param = vertex.getParam();
        vertex.getSoleEdge().fin(fin);
        // Reset
        param.reset();
    }
}
