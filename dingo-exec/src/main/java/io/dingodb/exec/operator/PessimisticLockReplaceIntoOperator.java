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
import io.dingodb.common.type.TupleType;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Status;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.exception.TaskCancelException;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PessimisticLockReplaceIntoParam;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.exec.utils.OpStateUtils;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.utils.ByteUtils.decodePessimisticKey;
import static io.dingodb.exec.utils.ByteUtils.encode;
import static io.dingodb.exec.utils.ByteUtils.getKeyByOp;
import static io.dingodb.exec.utils.ColumnDefaultValueUtils.getDefaultValue;

@Slf4j
public class PessimisticLockReplaceIntoOperator extends SoleOutOperator {
    public static final PessimisticLockReplaceIntoOperator INSTANCE = new PessimisticLockReplaceIntoOperator();

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            PessimisticLockReplaceIntoParam param = vertex.getParam();
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
            boolean isUnique = false;
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
                    } else if(originColumns.get(i).getSqlTypeName().equalsIgnoreCase("TINYINT")) {
                        if(tuple[i] instanceof Integer) {
                            if ( (Integer)tuple[i] >= 127 || (Integer)tuple[i] <= -128) {
                                throw new DingoTypeRangeException(0, "Out of range value for column '" + originColumns.get(i).getName() + "'");
                            }
                        }
                    }
                }
            }

            byte[] key = null;
            if (context.getIndexId() != null) {
                rowTuple = tuple;
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
                if (index.unique) {
                    throw new RuntimeException("Pessimistic transaction replace into unique index not support, " +
                        "please use optimistic transaction");
                }
                localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
                codec = CodecService.getDefault().createKeyValueCodec(
                    indexTable.getCodecVersion(), indexTable.version, indexTable.tupleType(), indexTable.keyMapping()
                );
            }
            StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
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
                    context.setReplaceIntoKey(true);
                } else {
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
                if (context.getIndexId() == null && !isVector && !isDocument) {
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
        PessimisticLockReplaceIntoParam param = vertex.getParam();
        vertex.getSoleEdge().fin(fin);
        // Reset
        param.reset();
    }
}
