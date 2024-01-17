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

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.data.Content;
import io.dingodb.exec.operator.params.PessimisticLockUpdateParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.store.api.transaction.exception.ReginSplitException;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class PessimisticLockUpdateOperator extends PartModifyOperator {
    public static final PessimisticLockUpdateOperator INSTANCE = new PessimisticLockUpdateOperator();

    @Override
    protected boolean pushTuple(Content content, @Nullable Object[] tuple, Vertex vertex) {
        PessimisticLockUpdateParam param = vertex.getParam();
        DingoType schema = param.getSchema();
        // add
        long startTs = param.getStartTs();
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId jobId = vertex.getTask().getJobId();
        CommonId tableId = param.getTableId();
        ITransaction transaction = TransactionManager.getTransaction(txnId);
        TupleMapping mapping = param.getMapping();
        List<SqlExpr> updates = param.getUpdates();
        int tupleSize = schema.fieldCount();
        Object[] newTuple = Arrays.copyOf(tuple, tupleSize);
        boolean updated = false;
        int i = 0;
        try {
            for (i = 0; i < mapping.size(); ++i) {
                Object newValue = updates.get(i).eval(tuple);
                int index = mapping.get(i);
                if ((newTuple[index] == null && newValue != null)
                    || (newTuple[index] != null && !newTuple[index].equals(newValue))
                ) {
                    newTuple[index] = newValue;
                    updated = true;
                }
            }
            Object[] newTuple2 = (Object[]) schema.convertFrom(newTuple, ValueConverter.INSTANCE);
            KeyValue keyValue = wrap(param.getCodec()::encode).apply(newTuple2);
            byte[] primaryKey = keyValue.getKey();
            CommonId partId = content.getDistribution().getId();
            Future future = null;
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
                .build();
            try {
                StoreInstance store = Services.KV_STORE.getInstance(tableId, partId);
                future = store.txnPessimisticLockPrimaryKey(txnPessimisticLock, param.getLockTimeOut());
            } catch (ReginSplitException e) {
                log.error(e.getMessage(), e);
                CommonId regionId = TransactionUtil.singleKeySplitRegionId(tableId, txnId, primaryKey);
                StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
                future = store.txnPessimisticLockPrimaryKey(txnPessimisticLock, param.getLockTimeOut());
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
                // primaryKeyLock rollback
                TransactionUtil.PessimisticPrimaryLockRollBack(
                    txnId,
                    tableId,
                    partId,
                    param.getIsolationLevel(),
                    startTs,
                    txnPessimisticLock.getForUpdateTs(),
                    primaryKey
                );
                throw new RuntimeException(e.getMessage());
            }
            if(future == null) {
                // primaryKeyLock rollback
                TransactionUtil.PessimisticPrimaryLockRollBack(
                    txnId,
                    tableId,
                    partId,
                    param.getIsolationLevel(),
                    startTs,
                    txnPessimisticLock.getForUpdateTs(),
                    primaryKey
                );
                throw new RuntimeException(txnId + " future is null " + partId + ",txnPessimisticLockPrimaryKey false");
            }
            long forUpdateTs = txnPessimisticLock.getForUpdateTs();
            transaction.setForUpdateTs(forUpdateTs);
            transaction.setPrimaryKeyFuture(future);
            StoreInstance store = Services.LOCAL_STORE.getInstance(tableId, partId);
            byte[] jobIdByte = jobId.encode();
            byte[] txnIdByte = txnId.encode();
            byte[] tableIdByte = tableId.encode();
            byte[] partIdByte = partId.encode();
            byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
            int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
            // lockKeyValue  [11_txnId_tableId_partId_a_lock, forUpdateTs1]
            transaction.setPrimaryKeyLock(
                ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_LOCK,
                    keyValue.getKey(),
                    Op.LOCK.getCode(),
                    len,
                    txnIdByte,
                    tableIdByte,
                    partIdByte)
            );
            // extraKeyValue  [12_jobId_tableId_partId_a_none, value]
            byte[] extraKeyBytes = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                keyValue.getKey(),
                Op.NONE.getCode(),
                len,
                jobIdByte,
                tableIdByte,
                partIdByte
            );
            KeyValue extraKeyValue = new KeyValue(extraKeyBytes, keyValue.getValue());
            // dataKeyValue   [10_txnId_tableId_partId_a_put, value]
            keyValue.setKey(
                ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_DATA,
                    keyValue.getKey(),
                    Op.PUT.getCode(),
                    len,
                    txnIdByte,
                    tableIdByte,
                    partIdByte)
            );
            if (store.put(extraKeyValue) && store.put(keyValue)) {
                param.inc();
            }
        } catch (Exception ex) {
            log.error("txn update operator with expr:{}, exception:{}",
                updates.get(i) == null ? "None" : updates.get(i).getExprString(),
                ex, ex);
            throw new RuntimeException("Txn_update Operator catch Exception");
        }
        return false;
    }
}
