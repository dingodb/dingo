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
import io.dingodb.exec.operator.data.Content;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.params.CleanCacheParam;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

@Slf4j
public class CleanCacheOperator extends TransactionOperator {
    public static final CleanCacheOperator INSTANCE = new CleanCacheOperator();

    private CleanCacheOperator() {
    }

    @Override
    public synchronized boolean push(Content content, @Nullable Object[] tuple, Vertex vertex) {
        CleanCacheParam param = vertex.getParam();
        StoreInstance store = Services.LOCAL_STORE.getInstance(null, null);
        KeyValue keyValue = (KeyValue) tuple[0];
        if (param.getTransactionType() == TransactionType.OPTIMISTIC) {
            store.deletePrefix(keyValue.getKey());
        } else {
            byte[] dataKey = keyValue.getKey();
            byte[] lockKey = Arrays.copyOf(dataKey, dataKey.length);
            lockKey[0] = (byte) CommonId.CommonType.TXN_CACHE_LOCK.getCode();
            lockKey[lockKey.length - 2] = (byte) Op.LOCK.getCode();
            KeyValue lockKeyValue = store.get(lockKey);
            long forUpdateTs = PrimitiveCodec.decodeLong(lockKeyValue.getValue());
            byte[] jobIdBytes = new CommonId(CommonId.CommonType.JOB, param.getStartTs(), forUpdateTs).encode();
            byte[] txnIdBytes = vertex.getTask().getTxnId().encode();
            // delete extraData
            byte[] extraData = new byte[CommonId.TYPE_LEN + jobIdBytes.length];
            extraData[0] = (byte) CommonId.CommonType.TXN_CACHE_EXTRA_DATA.getCode();
            System.arraycopy(txnIdBytes, 0, extraData, CommonId.TYPE_LEN, jobIdBytes.length);
            store.deletePrefix(extraData);
            // delete lockData
            store.deletePrefix(lockKey);
            // delete blockLock
            lockKey[0] = (byte) CommonId.CommonType.TXN_CACHE_BLOCK_LOCK.getCode();
            store.deletePrefix(lockKey);
            // delete dataKey
            store.deletePrefix(dataKey);
        }
        return true;
    }

    @Override
    public synchronized void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        if (!(fin instanceof FinWithException)) {
            vertex.getSoleEdge().transformToNext(new Object[]{true});
        }
        vertex.getSoleEdge().fin(fin);
    }

}
