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
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.store.KeyValue;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.params.OptimisticRollBackParam;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;


@Slf4j
public class OptimisticRollBackOperator extends TransactionOperator {
    public static final OptimisticRollBackOperator INSTANCE = new OptimisticRollBackOperator();

    private OptimisticRollBackOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            OptimisticRollBackParam param = vertex.getParam();
            KeyValue keyValue = (KeyValue) tuple[0];
            if (param.isClean()) {
                StoreInstance store = Services.LOCAL_STORE.getInstance(null, null);
                store.delete(keyValue.getKey());
                LogUtils.info(log, "Optimistic clean key is {}", Arrays.toString(keyValue.getKey()));
            } else {
                Object[] decode = ByteUtils.decode(keyValue);
                TxnLocalData txnLocalData = (TxnLocalData) decode[0];
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
                // first appearance
                if (op != Op.NONE.getCode()) {
                    // set oldKeyValue
                    byte[] newKey = Arrays.copyOf(dataKey, dataKey.length);
                    newKey[newKey.length - 2] = (byte) op;
                    store.put(new KeyValue(newKey, value));
                } else {
                    store.delete(ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_CHECK_DATA, Op.CheckNotExists, deleteKey));
                }
                store.delete(ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_EXTRA_DATA, Op.forNumber(op), deleteKey));
                LogUtils.info(log, "Optimistic RollBack key is {}, jobId:{}", Arrays.toString(key), jobId);
            }
            return true;
        }
    }


    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        synchronized (vertex) {
            if (!(fin instanceof FinWithException)) {
                vertex.getSoleEdge().transformToNext(new Object[]{true});
            }
            vertex.getSoleEdge().fin(fin);
        }
    }

}
