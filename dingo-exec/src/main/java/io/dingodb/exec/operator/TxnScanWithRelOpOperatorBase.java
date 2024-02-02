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

import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.CoprocessorV2;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnScanWithRelOpParam;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public abstract class TxnScanWithRelOpOperatorBase extends TxnScanOperatorBase {
    protected static @NonNull Iterator<KeyValue> createStoreIteratorCp(
        @NonNull CommonId tableId,
        @NonNull RangeDistribution distribution,
        long scanTs,
        long timeOut,
        CoprocessorV2 coprocessor
    ) {
        byte[] startKey = distribution.getStartKey();
        byte[] endKey = distribution.getEndKey();
        boolean includeStart = distribution.isWithStart();
        boolean includeEnd = distribution.isWithEnd();
        CommonId partId = distribution.getId();
        CodecService.getDefault().setId(startKey, partId.domain);
        CodecService.getDefault().setId(endKey, partId.domain);
        StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
        return kvStore.txnScan(
            scanTs,
            new StoreInstance.Range(startKey, endKey, includeStart, includeEnd),
            timeOut,
            coprocessor
        );
    }

    @Override
    protected @NonNull Iterator<Object[]> createIterator(@NonNull Context context, @NonNull Vertex vertex) {
        TxnScanWithRelOpParam param = vertex.getParam();
        CommonId tableId = param.getTableId();
        CommonId txnId = vertex.getTask().getTxnId();
        RangeDistribution distribution = context.getDistribution();
        Iterator<KeyValue> localIterator = createLocalIterator(txnId, tableId, distribution);
        if (localIterator.hasNext()) { // Cannot push down
            Iterator<KeyValue> storeIterator = createStoreIterator(
                tableId,
                distribution,
                param.getScanTs(),
                param.getTimeOut()
            );
            param.setCoprocessor(null);
            return createMergedIterator(localIterator, storeIterator, param.getCodec());
        }
        CoprocessorV2 coprocessor = param.getCoprocessor();
        if (coprocessor == null) {
            Iterator<KeyValue> storeIterator = createStoreIterator(
                tableId,
                distribution,
                param.getScanTs(),
                param.getTimeOut()
            );
            return Iterators.transform(storeIterator, wrap(param.getCodec()::decode)::apply);
        }
        Iterator<KeyValue> storeIterator = createStoreIteratorCp(
            tableId,
            distribution,
            param.getScanTs(),
            param.getTimeOut(),
            coprocessor
        );
        return Iterators.transform(storeIterator, wrap(param.getPushDownCodec()::decode)::apply);
    }
}
