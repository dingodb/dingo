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
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Status;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.exception.TaskCancelException;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PessimisticLockInsertParam;
import io.dingodb.exec.transaction.base.TxnPartData;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionUtil;
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
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.utils.ByteUtils.decodePessimisticKey;
import static io.dingodb.exec.utils.ByteUtils.encode;
import static io.dingodb.exec.utils.ByteUtils.getKeyByOp;

@Slf4j
public class PessimisticLockInsertOperator extends SoleOutOperator {
    public static final PessimisticLockInsertOperator INSTANCE = new PessimisticLockInsertOperator();

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            PessimisticLockInsertParam param = vertex.getParam();
            param.setContext(context);
            CommonId txnId = vertex.getTask().getTxnId();
            CommonId tableId = param.getTableId();
            CommonId partId = context.getDistribution().getId();
            byte[] primaryLockKey = param.getPrimaryLockKey();
            DingoType schema = param.getSchema();
            StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
            KeyValueCodec codec = param.getCodec();
            boolean isVector = false;
            boolean isDocument = false;
            Object[] rowTuple = null;

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
                    }
                }
            }

            byte[] key = null;
            if (context.getIndexId() != null) {
                rowTuple = tuple;
                TupleMapping updateMapping = param.getUpdateMapping();
                List<SqlExpr> updates = param.getUpdates();
                boolean duplicate = updateMapping != null && updates != null;
                Table indexTable = (Table) TransactionManager.getIndex(txnId, context.getIndexId());
                if (indexTable == null) {
                    LogUtils.error(log, "[ddl] Pessimistic insert get index table null, indexId:{}", context.getIndexId());
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
                Object[] finalTuple = tuple;
                Object finalDefaultVal = defaultVal;
                tuple = columnIndices.stream().map(i -> {
                    if (i == -1) {
                        return finalDefaultVal;
                    }
                    return finalTuple[i];
                }).toArray();
                schema = indexTable.tupleType();
                IndexTable index = (IndexTable) TransactionManager.getIndex(txnId, tableId);
                if (index.indexType.isVector) {
                    isVector = true;
                }
                if (index.indexType == IndexType.DOCUMENT) {
                    isDocument = true;
                }
                localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
                codec = CodecService.getDefault().createKeyValueCodec(
                    indexTable.getCodecVersion(), indexTable.version, indexTable.tupleType(), indexTable.keyMapping()
                );

                if (duplicate && context.getTablePartId() != null) {
                    Object[] primaryTuple = (Object[]) param.getSchema().convertFrom(finalTuple, ValueConverter.INSTANCE);
                    KeyValue primaryKv = wrap(param.getCodec()::encode).apply(primaryTuple);
                    StoreInstance store =
                        Services.KV_STORE.getInstance(param.getTable().tableId, context.getTablePartId());
                    KeyValue getPrimaryKv = store.txnGet(txnId.seq, primaryKv.getKey(), param.getLockTimeOut());

                    Object[] getPrimaryTuple = null;
                    if (getPrimaryKv == null || getPrimaryKv.getValue() == null) {
                        primaryTuple = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
                        byte[] originalKey;
                        if (isVector) {
                            originalKey = codec.encodeKeyPrefix(primaryTuple, 1);
                            CodecService.getDefault().setId(originalKey, partId.domain);
                        } else if (isDocument) {
                            originalKey = codec.encodeKeyPrefix(primaryTuple, 1);
                            CodecService.getDefault().setId(originalKey, partId.domain);
                        } else {
                            primaryKv = wrap(codec::encode).apply(primaryTuple);
                            originalKey = primaryKv.getKey();
                        }
                        store = Services.KV_STORE.getInstance(tableId, partId);
                        getPrimaryKv = store.txnGet(txnId.seq, originalKey, param.getLockTimeOut());
                        if (getPrimaryKv != null && getPrimaryKv.getValue() != null) {
                            getPrimaryTuple = codec.decode(getPrimaryKv);
                        }
                    } else {
                        getPrimaryTuple = param.getCodec().decode(getPrimaryKv);
                    }
                    boolean duplicateKey2 = false;
                    Object[] tmpTuple = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
                    for (int i = 0; i < index.keyMapping().size(); i++) {
                        int i1 = index.keyMapping().get(i);
                        if (getPrimaryTuple != null
                            && tmpTuple.length == getPrimaryTuple.length
                            && (tmpTuple[i1].equals(getPrimaryTuple[i1]) || tmpTuple[i1] == getPrimaryTuple[i1])
                            && !duplicateKey2) {
                            duplicateKey2 = true;
                        }
                    }
                    if ((getPrimaryKv != null && getPrimaryKv.getValue() != null) || getPrimaryTuple != null) {
                        context.setDuplicateKey(true);
                        if (param.isPessimisticTxn()) {
                            Object oldDefaultVal = null;
                            if (columnIndices.contains(-1)) {
                                Column addColumn = indexTable.getColumns().stream()
                                    .filter(column -> column.getSchemaState() != SchemaState.SCHEMA_PUBLIC)
                                    .findFirst().orElse(null);
                                if (addColumn != null) {
                                    oldDefaultVal = addColumn.getDefaultVal();
                                }
                            }
                            Object finalOldDefaultVal = oldDefaultVal;
                            Object[] finalGetPrimaryTuple = getPrimaryTuple;
                            boolean finalIsVector = isVector;
                            getPrimaryTuple = columnIndices.stream().map(i -> {
                                if (i == -1) {
                                    return finalOldDefaultVal;
                                }
                                if (finalIsVector) {
                                    return finalTuple[i];
                                }
                                return finalGetPrimaryTuple[i];
                            }).toArray();
                        }
                        boolean updated = false;
                        int[] indexMapping = new int[index.mapping().size()];
                        for (int i = 0; i < index.mapping().size(); i++) {
                            indexMapping[i] = updateMapping.findIdx(index.mapping.get(i));
                        }
                        TupleMapping tupleMapping = TupleMapping.of(indexMapping);
                        boolean duplicateKey = false;
                        for (int i = 0; i < tupleMapping.size(); i++) {
                            int i1 = tupleMapping.get(i);
                            Object newValue = null;
                            if (i1 >= 0) {
                                newValue = updates.get(i1) == null ? getPrimaryTuple[i1] : updates.get(i1).eval(finalTuple);
                            }
                            if (newValue != null && newValue.equals("NULL")) {
                                newValue = null;
                            }
                            int i2 = index.mapping().get(i);
                            if (getPrimaryTuple[i2] != null && getPrimaryTuple[i2].equals(newValue) && index.isUnique()) {
                                duplicateKey = true;
                                tuple[i2] = newValue;
                            }
                            if ((getPrimaryTuple[i2] != null && newValue != null && !getPrimaryTuple[i2].equals(newValue))
                                || (getPrimaryTuple[i2] == null && newValue != null)) {
                                // getPrimaryTuple[i2] = newValue;
                                if (tuple[i2] != null && !tuple[i2].equals(newValue)) {
                                    tuple[i2] = newValue;
                                }
                                updated = true;
                            }
                        }
                        if (updated) {
                            PartitionService ps = PartitionService.getService(
                                Optional.ofNullable(indexTable.getPartitionStrategy())
                                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                            KeyValue oldIndexKv = wrap(codec::encode).apply(getPrimaryTuple);
                            if (duplicateKey) {
                                throw new DuplicateEntryException("Duplicate entry "
                                    + TransactionUtil.duplicateEntryKey(tableId, oldIndexKv.getKey(), txnId) + " for key 'PRIMARY");
                            }
                            CommonId oldIndexPartId =
                                ps.calcPartId(oldIndexKv.getKey(), MetaService.root().getRangeDistribution(tableId));
                            StoreInstance indexLocalStore =
                                Services.LOCAL_STORE.getInstance(context.getIndexId(), oldIndexPartId);
                            byte[] oldKey = oldIndexKv.getKey();
                            byte[] txnIdByte = txnId.encode();
                            byte[] tableIdByte = tableId.encode();
                            byte[] partIdByte = oldIndexPartId.encode();
                            byte[] jobIdByte = vertex.getTask().getJobId().encode();
                            int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
                            CodecService.getDefault().setId(oldIndexKv.getKey(), oldIndexPartId.domain);
                            byte[] lockKeyBytes = encode(
                                CommonId.CommonType.TXN_CACHE_LOCK,
                                oldKey,
                                Op.LOCK.getCode(),
                                len,
                                txnIdByte,
                                tableIdByte,
                                partIdByte);
                            KeyValue keyValue = indexLocalStore.get(lockKeyBytes);
                            if (keyValue != null) {
                                byte[] dataKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_DATA, Op.PUT, lockKeyBytes);
                                byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
                                deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
                                byte[] insertKey = Arrays.copyOf(dataKey, dataKey.length);
                                insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
                                if (indexLocalStore.get(dataKey) != null) {
                                    byte[] value = indexLocalStore.get(dataKey).getValue();
                                    indexLocalStore.put(new KeyValue(deleteKey, value));
                                    indexLocalStore.delete(dataKey);

                                    KeyValue extraKeyValue = new KeyValue(
                                        ByteUtils.encode(
                                            CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                                            oldKey,
                                            Op.PUT.getCode(),
                                            len,
                                            jobIdByte,
                                            tableIdByte,
                                            partIdByte),
                                        value
                                    );
                                    indexLocalStore.put(extraKeyValue);
                                } else if (indexLocalStore.get(insertKey) != null) {
                                    byte[] value = indexLocalStore.get(insertKey).getValue();
                                    indexLocalStore.put(new KeyValue(deleteKey, value));
                                    indexLocalStore.delete(insertKey);

                                    KeyValue extraKeyValue = new KeyValue(
                                        ByteUtils.encode(
                                            CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                                            oldKey,
                                            Op.PUTIFABSENT.getCode(),
                                            len,
                                            jobIdByte,
                                            tableIdByte,
                                            partIdByte),
                                        value
                                    );
                                    indexLocalStore.put(extraKeyValue);
                                }
                            }
                            byte[] deadLockKeyBytes = encode(
                                CommonId.CommonType.TXN_CACHE_BLOCK_LOCK,
                                oldKey,
                                Op.LOCK.getCode(),
                                len,
                                txnIdByte,
                                tableIdByte,
                                partIdByte
                            );
                            KeyValue deadLockKeyValue = new KeyValue(deadLockKeyBytes, null);
                            indexLocalStore.put(deadLockKeyValue);
                            byte[] primaryLockKeyBytes = decodePessimisticKey(primaryLockKey);
                            long forUpdateTs = vertex.getTask().getJobId().seq;
                            byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
                            LogUtils.debug(log, "{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, forUpdateTs, Arrays.toString(oldKey));
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
                            TxnPessimisticLock txnPessimisticLock = TransactionUtil.getTxnPessimisticLock(
                                txnId,
                                tableId,
                                partId,
                                primaryLockKeyBytes,
                                oldKey,
                                param.getStartTs(),
                                forUpdateTs,
                                param.getIsolationLevel(),
                                true
                            );
                            try {
                                KeyValue kvKeyValue = TransactionUtil.pessimisticLock(
                                    txnPessimisticLock,
                                    param.getLockTimeOut(),
                                    txnId,
                                    tableId,
                                    partId,
                                    oldKey,
                                    param.isScan()
                                );
                                long newForUpdateTs = txnPessimisticLock.getForUpdateTs();
                                if (newForUpdateTs != forUpdateTs) {
                                    forUpdateTs = newForUpdateTs;
                                    forUpdateTsByte = PrimitiveCodec.encodeLong(newForUpdateTs);
                                }
                                LogUtils.debug(log, "{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, newForUpdateTs, Arrays.toString(oldKey));
                                if(vertex.getTask().getStatus() == Status.STOPPED) {
                                    TransactionUtil.resolvePessimisticLock(
                                        param.getIsolationLevel(),
                                        txnId,
                                        tableId,
                                        partId,
                                        deadLockKeyBytes,
                                        oldKey,
                                        param.getStartTs(),
                                        txnPessimisticLock.getForUpdateTs(),
                                        false,
                                        null
                                    );
                                    return false;
                                } else if (vertex.getTask().getStatus() == Status.CANCEL) {
                                    throw new TaskCancelException("task is cancel");
                                }
                                indexLocalStore.delete(deadLockKeyBytes);
                                byte[] lockKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                                // lockKeyValue
                                KeyValue lockKeyValue = new KeyValue(lockKey, forUpdateTsByte);
                                localStore.put(lockKeyValue);
                                if (kvKeyValue != null && kvKeyValue.getValue() != null) {
                                    // extraKeyValue
                                    KeyValue extraKeyValue = new KeyValue(
                                        ByteUtils.encode(
                                            CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                                            oldKey,
                                            Op.NONE.getCode(),
                                            len,
                                            jobIdByte,
                                            tableIdByte,
                                            partIdByte),
                                        kvKeyValue.getValue()
                                    );
                                    localStore.put(extraKeyValue);
                                    // data
                                    byte[] dataKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_DATA, Op.PUTIFABSENT, deadLockKeyBytes);
                                    localStore.delete(dataKey);
                                    byte[] updateKey = Arrays.copyOf(dataKey, dataKey.length);
                                    updateKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
                                    localStore.delete(updateKey);
                                    byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
                                    deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
                                    vertex.getTask().getPartData().put(
                                        new TxnPartData(tableId, partId),
                                        (!isVector && !isDocument)
                                    );
                                    localStore.put(new KeyValue(deleteKey, kvKeyValue.getValue()));
                                }
                            } catch (Throwable throwable) {
                                LogUtils.error(log, throwable.getMessage(), throwable);
                                TransactionUtil.resolvePessimisticLock(
                                    param.getIsolationLevel(),
                                    txnId,
                                    tableId,
                                    partId,
                                    deadLockKeyBytes,
                                    oldKey,
                                    param.getStartTs(),
                                    txnPessimisticLock.getForUpdateTs(),
                                    true,
                                    throwable
                                );
                            }
                            key = oldKey;
                        }
                        if (!updated && !duplicateKey && param.isDuplicateUpdate() && !duplicateKey2) {
                            return false;
                        }
                    }
                }
            }
            StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
            Object[] newTuple = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
            KeyValue keyValue = wrap(codec::encode).apply(newTuple);
            CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
            key = keyValue.getKey();


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

                byte[] primaryLockKeyBytes = decodePessimisticKey(primaryLockKey);
                long forUpdateTs = vertex.getTask().getJobId().seq;
                byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
                LogUtils.debug(log, "{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, forUpdateTs, Arrays.toString(key));
                if(vertex.getTask().getStatus() == Status.STOPPED) {
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
                TxnPessimisticLock txnPessimisticLock = TransactionUtil.getTxnPessimisticLock(
                    txnId,
                    tableId,
                    partId,
                    primaryLockKeyBytes,
                    key,
                    param.getStartTs(),
                    forUpdateTs,
                    param.getIsolationLevel(),
                    true
                );

                KeyValue kvKeyValue = null;
                try {
                    kvKeyValue = TransactionUtil.pessimisticLock(
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
                        forUpdateTsByte = PrimitiveCodec.encodeLong(newForUpdateTs);
                    }
                    LogUtils.debug(log, "{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, newForUpdateTs, Arrays.toString(key));
                    if(vertex.getTask().getStatus() == Status.STOPPED) {
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
                if (kvKeyValue != null && kvKeyValue.getValue() != null) {
                    if (param.isDuplicateUpdate()) {
                        context.setDuplicateKey(true);
                    }
                    if (!param.isReplaceInto() && !param.isIgnore()) {
                        TransactionUtil.resolvePessimisticLock(
                            param.getIsolationLevel(),
                            txnId,
                            tableId,
                            partId,
                            deadLockKeyBytes,
                            key,
                            param.getStartTs(),
                            forUpdateTs,
                            true,
                            new DuplicateEntryException("Duplicate entry " +
                                TransactionUtil.duplicateEntryKey(CommonId.decode(tableIdByte), key, txnId) + " for key 'PRIMARY'")
                        );
                    } else {
                        if (param.isReplaceInto()) {
                            context.setReplaceIntoKey(true);
                        }
                        if (param.isIgnore()) {
                            LogUtils.info(log, "PessimisticLockInsert RESIDUAL_LOCK jobId:{}",
                                CommonId.decode(jobIdByte));
                            byte[] rollBackKey = ByteUtils.getKeyByOp(
                                CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK,
                                Op.DELETE,
                                deadLockKeyBytes
                            );
                            localStore.put(new KeyValue(rollBackKey, kvKeyValue.getValue()));
                        }
                    }
                } else {
                    context.setDuplicateKey(false);
                    context.setReplaceIntoKey(false);
                }
                byte[] lockKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                // lockKeyValue
                KeyValue lockKeyValue = new KeyValue(lockKey, forUpdateTsByte);
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
                Object[] resultTuple = rowTuple == null ? newTuple : rowTuple;
                vertex.getOutList().forEach(o -> o.transformToNext(context, resultTuple));
            } else {
                if (param.isDuplicateUpdate() && ByteArrayUtils.compare(key, decodePessimisticKey(primaryLockKey)) == 0) {
                    byte[] dataKey = ByteUtils.encode(
                        CommonId.CommonType.TXN_CACHE_DATA,
                        key,
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
                    if (keyValues == null || keyValues.isEmpty()) {
                        KeyValue getKv = kvStore.txnGet(TsoService.getDefault().tso(), key, param.getLockTimeOut());
                        context.setDuplicateKey(getKv != null && getKv.getValue() != null);
                    }
                }
                if (param.isReplaceInto() && context.getIndexId() == null && !isVector && !isDocument) {
                    KeyValue kvKeyValue = kvStore.txnGet(
                        TsoService.getDefault().tso(), originalKey, param.getLockTimeOut()
                    );
                    if (kvKeyValue != null && kvKeyValue.getValue() != null) {
                        context.setReplaceIntoKey(true);
                    }
                }
                @Nullable Object[] resultTuple = rowTuple == null ? tuple : rowTuple;
                vertex.getOutList().forEach(o -> o.transformToNext(context, resultTuple));
            }
            return true;
        }
    }

    @Override
    public synchronized void fin(int pin, Fin fin, Vertex vertex) {
        PessimisticLockInsertParam param = vertex.getParam();
        vertex.getSoleEdge().fin(fin);
        // Reset
        param.reset();
    }
}
