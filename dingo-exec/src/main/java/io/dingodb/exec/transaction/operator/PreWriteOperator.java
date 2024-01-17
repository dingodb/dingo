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

package io.dingodb.exec.transaction.operator;

import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Content;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.params.PreWriteParam;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.exception.ReginSplitException;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

@Slf4j
public final class PreWriteOperator extends TransactionOperator {
    public static final PreWriteOperator INSTANCE = new PreWriteOperator();

    private PreWriteOperator() {
    }

    @Override
    public synchronized boolean push(Content content, @Nullable Object[] tuple, Vertex vertex) {
        PreWriteParam param = vertex.getParam();
        CommonId.CommonType type = CommonId.CommonType.of((byte) tuple[0]);
        CommonId txnId = (CommonId) tuple[1];
        CommonId tableId = (CommonId) tuple[2];
        CommonId newPartId = (CommonId) tuple[3];
        int op = (byte) tuple[4];
        byte[] key = (byte[]) tuple[5];
        byte[] value = (byte[]) tuple[6];
        // first key is primary key
        if (ByteArrayUtils.compare(key, param.getPrimaryKey(), 9) == 0) {
            return true;
        }
        StoreInstance store = Services.LOCAL_STORE.getInstance(tableId, newPartId);
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = tableId.encode();
        byte[] partIdByte = newPartId.encode();
        int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
        long forUpdateTs = 0;
        if (param.getTransactionType() == TransactionType.PESSIMISTIC) {
            byte[] lockBytes = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_LOCK,
                key,
                Op.LOCK.getCode(),
                len,
                txnIdByte, tableIdByte, partIdByte);
            KeyValue keyValue = store.get(lockBytes);
            if (keyValue == null) {
                throw new RuntimeException(txnId + " lock keyValue is null ");
            }
            forUpdateTs = (long) ByteUtils.decodePessimisticLock(keyValue)[6];
        }
        // cache to mutations
        Mutation mutation = TransactionCacheToMutation.cacheToMutation(op, key, value, forUpdateTs, tableId, newPartId);
        CommonId partId = param.getPartId();
        if (partId == null) {
            partId = newPartId;
            param.setPartId(partId);
            param.setTableId(tableId);
            param.addMutation(mutation);
        } else if (partId.equals(newPartId)) {
            param.addMutation(mutation);
            if (param.getMutations().size() == TransactionUtil.max_pre_write_count) {
                boolean result = txnPreWrite(param, txnId, tableId, partId);
                if (!result) {
                    throw new RuntimeException(txnId + " " + partId + ",txnPreWrite false,PrimaryKey:" + param.getPrimaryKey().toString());
                }
                param.getMutations().clear();
                param.setPartId(null);
            }
        } else {
            boolean result = txnPreWrite(param, txnId, tableId, partId);
            if (!result) {
                throw new RuntimeException(txnId + " " + partId + ",txnPreWrite false,PrimaryKey:" + param.getPrimaryKey().toString());
            }
            param.getMutations().clear();
            param.addMutation(mutation);
            param.setPartId(newPartId);
            param.setTableId(tableId);
        }
        return true;
    }

    private boolean txnPreWrite(PreWriteParam param, CommonId txnId, CommonId tableId, CommonId partId) {
        // 1、call sdk TxnPreWrite
        param.setTxnSize(param.getMutations().size());
        TxnPreWrite txnPreWrite;
        if (param.getTransactionType() == TransactionType.OPTIMISTIC) {
            txnPreWrite = TxnPreWrite.builder()
                .isolationLevel(IsolationLevel.of(param.getIsolationLevel()))
                .mutations(param.getMutations())
                .primaryLock(param.getPrimaryKey())
                .startTs(param.getStartTs())
                .lockTtl(TransactionManager.lockTtlTm())
                .txnSize(param.getTxnSize())
                .tryOnePc(param.isTryOnePc())
                .maxCommitTs(param.getMaxCommitTs())
                .lockExtraDatas(TransactionUtil.toLockExtraDataList(
                    tableId,
                    partId,
                    txnId,
                    param.getTransactionType().getCode(),
                    param.getMutations().size())
                )
                .build();
        } else {
            // ToDo Non-unique indexes do not require pessimistic locks and are equivalent to optimistic transactions
            txnPreWrite = TxnPreWrite.builder()
                .isolationLevel(IsolationLevel.of(param.getIsolationLevel()))
                .mutations(param.getMutations())
                .primaryLock(param.getPrimaryKey())
                .startTs(param.getStartTs())
                .lockTtl(TransactionManager.lockTtlTm())
                .txnSize(param.getTxnSize())
                .tryOnePc(param.isTryOnePc())
                .maxCommitTs(param.getMaxCommitTs())
                .pessimisticChecks(TransactionUtil.toPessimisticCheck(param.getMutations().size()))
                .forUpdateTsChecks(TransactionUtil.toForUpdateTsChecks(param.getMutations()))
                .lockExtraDatas(TransactionUtil.toLockExtraDataList(
                    tableId,
                    partId,
                    txnId,
                    param.getTransactionType().getCode(),
                    param.getMutations().size())
                )
                .build();
        }
        try {
            StoreInstance store = Services.KV_STORE.getInstance(tableId, partId);
            return store.txnPreWrite(txnPreWrite, param.getTimeOut());
        } catch (ReginSplitException e) {
            log.error(e.getMessage(), e);
            // 2、regin split
            Map<CommonId, List<byte[]>> partMap = TransactionUtil.multiKeySplitRegionId(
                tableId,
                txnId,
                TransactionUtil.mutationToKey(param.getMutations())
            );
            for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                CommonId regionId = entry.getKey();
                List<byte[]> value = entry.getValue();
                StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
                txnPreWrite.setMutations(TransactionUtil.keyToMutation(value, param.getMutations()));
                boolean result = store.txnPreWrite(txnPreWrite, param.getTimeOut());
                if (!result) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public synchronized void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        if (!(fin instanceof FinWithException)) {
            PreWriteParam param = vertex.getParam();
            if (param.getMutations().size() > 0) {
                boolean result = txnPreWrite(
                    param,
                    vertex.getTask().getTxnId(),
                    param.getTableId(),
                    param.getPartId()
                );
                if (!result) {
                    throw new RuntimeException(vertex.getTask().getTxnId() + " " + param.getPartId()
                        + ",txnPreWrite false,PrimaryKey:" + param.getPrimaryKey().toString());
                }
                param.getMutations().clear();
            }
            vertex.getSoleEdge().transformToNext(new Object[]{true});
        }
        vertex.getSoleEdge().fin(fin);
    }

}
