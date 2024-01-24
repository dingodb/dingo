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
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnPartRangeDeleteParam;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public final class TxnPartRangeDeleteOperator extends SoleOutOperator {
    public static final TxnPartRangeDeleteOperator INSTANCE = new TxnPartRangeDeleteOperator();

    private TxnPartRangeDeleteOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        RangeDistribution distribution = context.getDistribution();
        TxnPartRangeDeleteParam param = vertex.getParam();
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId tableId = param.getTableId();
        CommonId partId = distribution.getId();
        boolean withStart = distribution.isWithStart();
        boolean withEnd = distribution.isWithEnd();
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
        StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
        final long startTime = System.currentTimeMillis();
        // TODO Set flag in front of the byte key
        byte[] txnIdBytes = txnId.encode();
        byte[] tableIdBytes = tableId.encode();
        byte[] partIdBytes = partId.encode();
        byte[] encodeStart = ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_DATA,
            distribution.getStartKey(),
            Op.DELETE.getCode(),
            (txnIdBytes.length + tableIdBytes.length + partIdBytes.length),
            txnIdBytes,
            tableIdBytes,
            partIdBytes
        );
        byte[] encodeEnd = ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_DATA,
            distribution.getEndKey(),
            Op.DELETE.getCode(),
            (txnIdBytes.length + tableIdBytes.length + partIdBytes.length),
            txnIdBytes,
            tableIdBytes,
            partIdBytes
        );
        long count = localStore.delete(new StoreInstance.Range(encodeStart, encodeEnd, withStart, withEnd));
        kvStore.delete(new StoreInstance.Range(encodeStart, encodeEnd, withStart, withEnd));

        vertex.getSoleEdge().transformToNext(context, new Object[]{count});
        if (log.isDebugEnabled()) {
            log.debug("Delete data by range, delete count: {}, cost: {} ms.",
                count, System.currentTimeMillis() - startTime);
        }
        return false;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        vertex.getSoleEdge().fin(fin);
    }
}
