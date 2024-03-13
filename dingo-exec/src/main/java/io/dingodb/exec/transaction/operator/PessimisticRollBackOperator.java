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
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.params.PessimisticRollBackParam;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.rollback.TxnPessimisticRollBack;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.dingodb.exec.utils.ByteUtils.getKeyByOp;


@Slf4j
public class PessimisticRollBackOperator extends TransactionOperator {
    public static final PessimisticRollBackOperator INSTANCE = new PessimisticRollBackOperator();

    private PessimisticRollBackOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            PessimisticRollBackParam param = vertex.getParam();
            TxnLocalData txnLocalData = (TxnLocalData) tuple[0];
            CommonId.CommonType type = txnLocalData.getDataType();
            CommonId jobId = txnLocalData.getJobId();
            CommonId tableId = txnLocalData.getTableId();
            CommonId newPartId = txnLocalData.getPartId();
            int op = txnLocalData.getOp().getCode();
            byte[] key = txnLocalData.getKey();
            byte[] value = txnLocalData.getValue();
            CommonId txnId = vertex.getTask().getTxnId();
            StoreInstance store = Services.LOCAL_STORE.getInstance(tableId, newPartId);
            byte[] txnIdByte = txnId.encode();
            byte[] tableIdByte = tableId.encode();
            byte[] partIdByte = newPartId.encode();
            int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
            // cache delete key
            byte[] dataKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                key,
                Op.PUTIFABSENT.getCode(),
                len,
                txnIdByte, tableIdByte, partIdByte);
            store.delete(dataKey);
            byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
            deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
            store.delete(deleteKey);
            deleteKey[deleteKey.length - 2] = (byte) Op.PUT.getCode();
            store.delete(deleteKey);
            byte[] lockKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deleteKey);
            KeyValue keyValue = store.get(lockKey);
            long forUpdateTs;
            if (keyValue != null || keyValue.getValue() != null) {
                // forUpdateTs
                forUpdateTs = PrimitiveCodec.decodeLong(keyValue.getValue());
            } else {
                forUpdateTs = param.getForUpdateTs();
            }
            store.delete(lockKey);
            // delete blockLock
            lockKey[0] = (byte) CommonId.CommonType.TXN_CACHE_BLOCK_LOCK.getCode();
            store.delete(lockKey);
            // first appearance
            if (op != Op.NONE.getCode()) {
                // set oldKeyValue
                byte[] newKey = Arrays.copyOf(dataKey, dataKey.length);
                newKey[newKey.length - 2] = (byte) op;
                store.put(new KeyValue(newKey, value));
            }
            CommonId partId = param.getPartId();
            if (partId == null) {
                partId = newPartId;
                param.setPartId(partId);
                param.setTableId(tableId);
                param.addKey(key);
                param.setForUpdateTs(forUpdateTs);
            } else if (partId.equals(newPartId) && forUpdateTs == param.getForUpdateTs()) {
                param.addKey(key);
                if (param.getKeys().size() == TransactionUtil.max_pre_write_count) {
                    boolean result = txnPessimisticRollBack(param, txnId, tableId, partId);
                    if (!result) {
                        throw new RuntimeException(txnId + " " + partId + ",txnPessimisticRollBack false");
                    }
                    param.getKeys().clear();
                    param.setPartId(null);
                    param.setForUpdateTs(forUpdateTs);
                }
            } else {
                boolean result = txnPessimisticRollBack(param, txnId, param.getTableId(), partId);
                if (!result) {
                    throw new RuntimeException(txnId + " " + partId + ",txnPessimisticRollBack false");
                }
                param.getKeys().clear();
                param.addKey(key);
                param.setPartId(newPartId);
                param.setTableId(tableId);
                param.setForUpdateTs(forUpdateTs);
            }
            return true;
        }
    }

    private boolean txnPessimisticRollBack(PessimisticRollBackParam param, CommonId txnId, CommonId tableId, CommonId newPartId) {
        // 1、Async call sdk TxnPessimisticRollBack
        TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder()
            .isolationLevel(IsolationLevel.of(param.getIsolationLevel()))
            .startTs(param.getStartTs())
            .forUpdateTs(param.getForUpdateTs())
            .keys(param.getKeys())
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            return store.txnPessimisticLockRollback(pessimisticRollBack);
        } catch (RegionSplitException e) {
            log.error(e.getMessage(), e);
            // 2、regin split
            Map<CommonId, List<byte[]>> partMap = TransactionUtil.multiKeySplitRegionId(tableId, txnId, param.getKeys());
            for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                CommonId regionId = entry.getKey();
                List<byte[]> value = entry.getValue();
                StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
                pessimisticRollBack.setKeys(value);
                boolean result = store.txnPessimisticLockRollback(pessimisticRollBack);
                if (!result) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        synchronized (vertex) {
            if (!(fin instanceof FinWithException)) {
                PessimisticRollBackParam param = vertex.getParam();
                if (param.getKeys().size() > 0) {
                    CommonId txnId = vertex.getTask().getTxnId();
                    boolean result = txnPessimisticRollBack(param, txnId, param.getTableId(), param.getPartId());
                    if (!result) {
                        throw new RuntimeException(txnId + " " + param.getPartId() + ",txnPessimisticRollBack false");
                    }
                    param.getKeys().clear();
                }
                vertex.getSoleEdge().transformToNext(new Object[]{true});
            }
            vertex.getSoleEdge().fin(fin);
        }
    }

}
