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
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Status;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.exception.TaskCancelException;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.ForUpdateParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.utils.ByteUtils.decodePessimisticKey;
import static io.dingodb.exec.utils.ByteUtils.encode;
import static io.dingodb.exec.utils.ByteUtils.getKeyByOp;

@Slf4j
public class ForUpdateOperator extends SoleOutOperator {
    public static final ForUpdateOperator INSTANCE = new ForUpdateOperator();

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        ForUpdateParam param = vertex.getParam();
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId tableId = param.getTableId();
        CommonId partId = context.getDistribution().getId();
        CommonId jobId = vertex.getTask().getJobId();
        byte[] primaryLockKey = param.getPrimaryLockKey();
        ITransaction transaction = TransactionManager.getTransaction(txnId);
        if (transaction == null) {
            return false;
        }
        DingoType schema = param.getSchema();
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
        KeyValueCodec codec = param.getCodec();
        if (context.getIndexId() != null) {
            Table indexTable = (Table) TransactionManager.getIndex(txnId, context.getIndexId());
            if (indexTable == null) {
                LogUtils.error(log, "[ddl] Pessimistic for update get index table null, indexId:{}", context.getIndexId());
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
            localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
            codec = CodecService.getDefault().createKeyValueCodec(
                indexTable.codecVersion,
                indexTable.version, indexTable.tupleType(), indexTable.keyMapping()
            );
        }
        Object[] newTuple = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
        KeyValue keyValue = wrap(codec::encode).apply(newTuple);
        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
        byte[] key = keyValue.getKey();
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = tableId.encode();
        byte[] partIdByte = partId.encode();
        byte[] jobIdByte = jobId.encode();
        int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
        if (transaction.isPessimistic()) {
            if (primaryLockKey == null && transaction.getPrimaryKey() != null) {
                return false;
            }
            byte[] lockKeyBytes = encode(
                CommonId.CommonType.TXN_CACHE_LOCK,
                key,
                Op.LOCK.getCode(),
                len,
                txnIdByte,
                tableIdByte,
                partIdByte);
            KeyValue oldKeyValue = localStore.get(lockKeyBytes);
            if (oldKeyValue == null) {
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
                long forUpdateTs = jobId.seq;
                byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
                LogUtils.debug(log, "{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, forUpdateTs, Arrays.toString(key));
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
                        return vertex.getSoleEdge().transformToNext(context, tuple);
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
                byte[] lockKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                // lockKeyValue
                KeyValue lockKeyValue = new KeyValue(lockKey, forUpdateTsByte);
                localStore.put(lockKeyValue);
                // extraKeyValue
                KeyValue extraKeyValue = new KeyValue(
                    encode(
                        CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                        key, Op.NONE.getCode(),
                        len,
                        jobIdByte,
                        tableIdByte,
                        partIdByte),
                    (kvKeyValue != null && kvKeyValue.getValue() != null) ? kvKeyValue.getValue() : keyValue.getValue()
                );
                localStore.put(extraKeyValue);
                byte[] rollBackKey = getKeyByOp(
                    CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.LOCK, deadLockKeyBytes
                );
                localStore.put(new KeyValue(rollBackKey, null));
                @Nullable Object[] finalTuple1 = tuple;
                vertex.getOutList().forEach(o -> o.transformToNext(context, finalTuple1));
            } else {
                @Nullable Object[] finalTuple2 = tuple;
                vertex.getOutList().forEach(o -> o.transformToNext(context, finalTuple2));
            }
        } else {
            // Optimistic
            // extra key value
            KeyValue extraKeyValue = new KeyValue(
                encode(
                    CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                    key, Op.NONE.getCode(),
                    len,
                    jobIdByte,
                    tableIdByte,
                    partIdByte),
                keyValue.getValue()
            );
            localStore.put(extraKeyValue);

            byte[] dataKey = encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                key,
                Op.PUT.getCode(),
                len,
                txnIdByte,
                tableIdByte,
                partIdByte);
            keyValue.setKey(ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_DATA, Op.DELETE, dataKey));
            localStore.put(keyValue);
            return !context.isShow();
        }
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        vertex.getSoleEdge().fin(fin);
    }
}
