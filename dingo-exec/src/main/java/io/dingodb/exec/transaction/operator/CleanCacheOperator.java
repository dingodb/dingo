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
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.params.CleanCacheParam;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Iterator;

import static io.dingodb.store.api.transaction.data.Op.DELETE;

@Slf4j
public class CleanCacheOperator extends TransactionOperator {
    public static final CleanCacheOperator INSTANCE = new CleanCacheOperator();

    private CleanCacheOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            CleanCacheParam param = vertex.getParam();
            StoreInstance store = Services.LOCAL_STORE.getInstance(null, null);
            KeyValue keyValue = (KeyValue) tuple[0];
            if (param.getTransactionType() == TransactionType.OPTIMISTIC) {
                byte[] key = keyValue.getKey();
                store.delete(key);
                store.delete(ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_CHECK_DATA, Op.CheckNotExists, key));
            } else {
                byte[] lockKey = keyValue.getKey();
                long forUpdateTs = ByteUtils.decodePessimisticLockValue(keyValue);
                byte[] dataKey = Arrays.copyOf(lockKey, lockKey.length);
                dataKey[0] = (byte) CommonId.CommonType.TXN_CACHE_DATA.getCode();
                dataKey[dataKey.length - 2] = (byte) DELETE.getCode();
                // delete dataKey
                store.delete(dataKey);
                dataKey[dataKey.length - 2] = (byte) Op.PUT.getCode();
                // delete dataKey
                store.delete(dataKey);
                dataKey[dataKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
                // delete dataKey
                store.delete(dataKey);
                byte[] jobIdBytes = new CommonId(CommonId.CommonType.JOB, param.getStartTs(), forUpdateTs).encode();
                byte[] txnIdBytes = vertex.getTask().getTxnId().encode();
                // delete extraData
                byte[] extraData = new byte[CommonId.TYPE_LEN + jobIdBytes.length];
                extraData[0] = (byte) CommonId.CommonType.TXN_CACHE_EXTRA_DATA.getCode();
                System.arraycopy(txnIdBytes, 0, extraData, CommonId.TYPE_LEN, jobIdBytes.length);
                store.delete(extraData);
                // delete lockData
                store.delete(lockKey);
                // delete blockLock
                lockKey[0] = (byte) CommonId.CommonType.TXN_CACHE_BLOCK_LOCK.getCode();
                store.delete(lockKey);
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
