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
import io.dingodb.common.profile.SourceProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnScanParam;
import io.dingodb.exec.utils.RelOpUtils;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.DingoTransformedIterator;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public final class TxnScanOperator extends TxnScanOperatorBase {
    public static TxnScanOperator INSTANCE = new TxnScanOperator();

    @Override
    protected @NonNull Iterator<Object[]> createIterator(@NonNull Context context, @NonNull Vertex vertex) {
        TxnScanParam param = vertex.getParam();
        SourceProfile profile = param.getSourceProfile("scanBase");
        long start = System.currentTimeMillis();
        CommonId tableId = param.getTableId();
        Table table = DdlService.root().getTable(tableId);
        if (table != null) {
            boolean withoutPrimary = table.getColumns()
                .stream().anyMatch(column -> column.isPrimary() && column.getState() == 2);
            if (withoutPrimary) {
                context.setWithoutPrimary(true);
            }
        }
        CommonId txnId = vertex.getTask().getTxnId();
        RangeDistribution distribution = context.getDistribution();
        if (param.isAutoCommit()) {
            start = System.currentTimeMillis();
            Iterator<KeyValue> storeIterator = createStoreIterator(
                tableId,
                distribution,
                param.getScanTs(),
                param.getTimeOut()
            );
            profile.incrTxnScanTime(start);
            profile.end();
            return DingoTransformedIterator.transform(storeIterator, wrap(param.getCodec()::decode)::apply);
        }
        Iterator<KeyValue> localIterator = createLocalIterator(txnId, tableId, distribution);
        profile.incrLocalTime(start);
        start = System.currentTimeMillis();
        Iterator<KeyValue> storeIterator = createStoreIterator(
            tableId,
            distribution,
            param.getScanTs(),
            param.getTimeOut()
        );
        profile.incrTxnScanTime(start);
        Iterator<Object[]> res = createMergedIterator(localIterator, storeIterator, param.getCodec());
        profile.end();
        return res;
    }

    @Override
    protected @NonNull Scanner getScanner(@NonNull Context context, @NonNull Vertex vertex) {
        return RelOpUtils::doScan;
    }
}
