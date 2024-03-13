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
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PessimisticLockUpdateParam;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.utils.ByteUtils.decode;
import static io.dingodb.exec.utils.ByteUtils.decodePessimisticKey;
import static io.dingodb.exec.utils.ByteUtils.encode;
import static io.dingodb.exec.utils.ByteUtils.getKeyByOp;

@Slf4j
public class PessimisticLockUpdateOperator extends SoleOutOperator {
    public static final PessimisticLockUpdateOperator INSTANCE = new PessimisticLockUpdateOperator();

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            PessimisticLockUpdateParam param = vertex.getParam();
            param.setContext(context);
            CommonId txnId = vertex.getTask().getTxnId();
            CommonId tableId = param.getTableId();
            CommonId partId = context.getDistribution().getId();
            CommonId jobId = vertex.getTask().getJobId();
            byte[] primaryLockKey = param.getPrimaryLockKey();
            DingoType schema = param.getSchema();
            StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
            KeyValueCodec codec = param.getCodec();
            int tupleSize = schema.fieldCount();
            Object[] newTuple = Arrays.copyOf(tuple, tupleSize);
            Object[] copyTuple = Arrays.copyOf(tuple, tuple.length);
            TupleMapping mapping = param.getMapping();
            List<SqlExpr> updates = param.getUpdates();
            boolean updated = false;
            for (int i = 0; i < mapping.size(); ++i) {
                Object newValue = updates.get(i).eval(tuple);
                int index = mapping.get(i);
                if ((newTuple[index] == null && newValue != null)
                    || (newTuple[index] != null && !newTuple[index].equals(newValue))
                ) {
                    newTuple[index] = newValue;
                    updated = true;
                }
            }
            boolean isVector = false;
            boolean calcPartId = false;
            Object[] oldIndexTuple = tuple;
            if (context.getIndexId() != null) {
                Table indexTable = MetaService.root().getTable(context.getIndexId());
                List<Integer> columnIndices = param.getTable().getColumnIndices(indexTable.columns.stream()
                    .map(Column::getName)
                    .collect(Collectors.toList()));
                tableId = context.getIndexId();
                // old key
                Object[] finalTuple = tuple;
                tuple = columnIndices.stream().map(i -> finalTuple[i]).toArray();
                oldIndexTuple = Arrays.copyOf(tuple, tuple.length);
                if (updated) {
                    Object[] finalNewIndexTuple = newTuple;
                    tuple = columnIndices.stream().map(i -> finalNewIndexTuple[i]).toArray();
                }
                schema = indexTable.tupleType();
                IndexTable index = TransactionUtil.getIndexDefinitions(tableId);
                if (index.indexType.isVector) {
                    isVector = true;
                }
                codec = CodecService.getDefault().createKeyValueCodec(indexTable.tupleType(), indexTable.keyMapping());
                if (updated && columnIndices.stream().anyMatch(c -> mapping.contains(c))) {
                    PartitionService ps = PartitionService.getService(
                        Optional.ofNullable(indexTable.getPartitionStrategy())
                            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                    byte[] key = wrap(codec::encodeKey).apply(tuple);
                    partId = ps.calcPartId(key, MetaService.root().getRangeDistribution(tableId));
                    log.info("{} update lock index primary key is{} calcPartId is {}",
                        txnId,
                        Arrays.toString(key),
                        partId
                    );
                    calcPartId = true;
                }
            }
            localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
            StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);

            Object[] dest = new Object[schema.fieldCount()];
            System.arraycopy(tuple, 0, dest, 0, schema.fieldCount());
            dest = (Object[]) schema.convertFrom(dest, ValueConverter.INSTANCE);

            byte[] key = wrap(codec::encodeKey).apply(dest);
            CodecService.getDefault().setId(key, partId.domain);
            byte[] vectorKey;
            if (isVector) {
                vectorKey = codec.encodeKeyPrefix(dest, 1);
                CodecService.getDefault().setId(vectorKey, partId.domain);
            } else {
                vectorKey = key;
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
                if (calcPartId) {
                    resolveKeyChange(vertex, param, txnId, tableId, context.getDistribution().getId(), primaryLockKey,
                        codec, oldIndexTuple, txnIdByte, tableIdByte, jobIdByte, len, isVector, key);
                }
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
                if (log.isDebugEnabled()) {
                    log.info("{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, forUpdateTs, Arrays.toString(key));
                }
                try {
                    TxnPessimisticLock txnPessimisticLock = TransactionUtil.pessimisticLock(
                        param.getLockTimeOut(),
                        txnId,
                        tableId,
                        partId,
                        primaryLockKeyBytes,
                        key,
                        param.getStartTs(),
                        forUpdateTs,
                        param.getIsolationLevel()
                    );
                    long newForUpdateTs = txnPessimisticLock.getForUpdateTs();
                    if (newForUpdateTs != forUpdateTs) {
                        forUpdateTs = newForUpdateTs;
                        forUpdateTsByte = PrimitiveCodec.encodeLong(newForUpdateTs);
                    }
                    if (log.isDebugEnabled()) {
                        log.info("{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, newForUpdateTs, Arrays.toString(key));
                    }
                } catch (Throwable throwable) {
                    log.error(throwable.getMessage(), throwable);
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
                        throwable
                    );
                }
                // get lock success, delete deadLockKey
                localStore.delete(deadLockKeyBytes);
                byte[] lockKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                // lockKeyValue
                KeyValue lockKeyValue = new KeyValue(lockKey, forUpdateTsByte);
                localStore.put(lockKeyValue);
                KeyValue kvKeyValue = null;
                try {
                    // index use keyPrefix
                    kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), vectorKey, param.getLockTimeOut());
                } catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                } finally {
                    if (kvKeyValue != null && kvKeyValue.getValue() != null) {
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
                            kvKeyValue.getValue()
                        );
                        localStore.put(extraKeyValue);
                    }
                }
                if (context.getIndexId() != null) {
                    log.info("{}, txnPessimisticLock :{} , index is not null", txnId, Arrays.toString(key));
                    vertex.getOutList().forEach(o -> o.transformToNext(context, copyTuple));
                    return true;
                }
                if (kvKeyValue == null || kvKeyValue.getValue() == null) {
                    byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, deadLockKeyBytes);
                    localStore.put(new KeyValue(rollBackKey, null));
                    @Nullable Object[] finalTuple1 = tuple;
                    vertex.getOutList().forEach(o -> o.transformToNext(context, finalTuple1));
                    return true;
                }
                if (isVector) {
                    kvKeyValue.setKey(codec.encodeKey(dest));
                }
                Object[] result = codec.decode(kvKeyValue);
                vertex.getOutList().forEach(o -> o.transformToNext(context, result));
                return true;
            } else {
                byte[] dataKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_DATA, Op.PUT, lockKeyBytes);
                byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
                deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
                byte[] updateKey = Arrays.copyOf(dataKey, dataKey.length);
                updateKey[updateKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
                List<byte[]> bytes = new ArrayList<>(3);
                bytes.add(dataKey);
                bytes.add(deleteKey);
                bytes.add(updateKey);
                List<KeyValue> keyValues = localStore.get(bytes);
                byte[] primaryLockKeyBytes = decodePessimisticKey(primaryLockKey);
                if (keyValues != null && keyValues.size() > 0) {
                    if (keyValues.size() > 1) {
                        throw new RuntimeException(txnId + " Key is not existed than two in local localStore");
                    }
                    KeyValue value = keyValues.get(0);
                    byte[] oldKey = value.getKey();
                    log.info("{}, repeat key :{}", txnId, Arrays.toString(oldKey));
                    if (oldKey[oldKey.length - 2] == Op.DELETE.getCode()) {
                        return true;
                    }
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
                        extraKeyValue = new KeyValue(extraKey, Arrays.copyOf(value.getValue(), value.getValue().length));
                    }
                    localStore.put(extraKeyValue);
                    if (context.getIndexId() != null) {
                        log.info("{}, repeat primary key :{} keyValue is not null, index is not null", txnId, Arrays.toString(key));
                        vertex.getOutList().forEach(o -> o.transformToNext(context, copyTuple));
                        return true;
                    }
                    Object[] decode = decode(value);
                    KeyValue keyValue = new KeyValue(((TxnLocalData) decode[0]).getKey(), value.getValue());
                    Object[] result = codec.decode(keyValue);
                    vertex.getOutList().forEach(o -> o.transformToNext(context, result));
                    return true;
                } else {
                    if (context.getIndexId() != null) {
                        log.info("{}, repeat primary key :{} keyValue is not null, index is not null", txnId, Arrays.toString(key));
                        vertex.getOutList().forEach(o -> o.transformToNext(context, copyTuple));
                        return true;
                    }
                    KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), vectorKey, param.getLockTimeOut());
                    if (kvKeyValue == null || kvKeyValue.getValue() == null) {
                        log.info("{}, repeat primary key :{} keyValue is null", txnId, Arrays.toString(primaryLockKeyBytes));
                        @Nullable Object[] finalTuple1 = tuple;
                        vertex.getOutList().forEach(o -> o.transformToNext(context, finalTuple1));
                        return true;
                    }
                    log.info("{}, repeat primary key :{} keyValue is not null", txnId, Arrays.toString(key));
                    if (isVector) {
                        kvKeyValue.setKey(codec.encodeKey(dest));
                    }
                    Object[] result = codec.decode(kvKeyValue);
                    vertex.getOutList().forEach(o -> o.transformToNext(context, result));
                    return true;
                }
            }
        }
    }

    private void resolveKeyChange(Vertex vertex, PessimisticLockUpdateParam param, CommonId txnId,
                                  CommonId tableId, CommonId partId, byte[] primaryLockKey,
                                  KeyValueCodec codec, Object[] newTuple, byte[] txnIdByte,
                                  byte[] tableIdByte, byte[] jobIdByte, int len, boolean isVector, byte[] key) {
        byte[] oldKey = wrap(codec::encodeKey).apply(newTuple);
        CodecService.getDefault().setId(oldKey, partId.domain);
        if (ByteArrayUtils.equal(key, oldKey)) {
            return;
        }
        byte[] vectorKey;
        if (isVector) {
            vectorKey = codec.encodeKeyPrefix(newTuple, 1);
            CodecService.getDefault().setId(vectorKey, partId.domain);
        } else {
            vectorKey = oldKey;
        }
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
        StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
        byte[] partIdByte = partId.encode();
        // for check deadLock
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
        localStore.put(deadLockKeyValue);
        byte[] primaryLockKeyBytes = decodePessimisticKey(primaryLockKey);
        long forUpdateTs = vertex.getTask().getJobId().seq;
        byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
        if (log.isDebugEnabled()) {
            log.info("{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, forUpdateTs, Arrays.toString(oldKey));
        }
        TxnPessimisticLock txnPessimisticLock = TransactionUtil.pessimisticLock(
            param.getLockTimeOut(),
            txnId,
            tableId,
            partId,
            primaryLockKeyBytes,
            oldKey,
            param.getStartTs(),
            forUpdateTs,
            param.getIsolationLevel()
        );
        long newForUpdateTs = txnPessimisticLock.getForUpdateTs();
        if (newForUpdateTs != forUpdateTs) {
            forUpdateTsByte = PrimitiveCodec.encodeLong(newForUpdateTs);
        }
        if (log.isDebugEnabled()) {
            log.info("{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, newForUpdateTs, Arrays.toString(oldKey));
        }
        // get lock success, delete deadLockKey
        localStore.delete(deadLockKeyBytes);
        byte[] lockKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
        // lockKeyValue
        KeyValue lockKeyValue = new KeyValue(lockKey, forUpdateTsByte);
        localStore.put(lockKeyValue);
        KeyValue kvKeyValue = null;
        try {
            // index use keyPrefix
            kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), vectorKey, param.getLockTimeOut());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        } finally {
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
                localStore.put(new KeyValue(deleteKey, kvKeyValue.getValue()));
            }
        }
    }


    @Override
    public synchronized void fin(int pin, Fin fin, Vertex vertex) {
        PessimisticLockUpdateParam param = vertex.getParam();
        vertex.getSoleEdge().fin(fin);
        // Reset
        param.reset();
    }
}
