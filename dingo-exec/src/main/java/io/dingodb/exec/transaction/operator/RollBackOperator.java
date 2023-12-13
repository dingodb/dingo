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
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.transaction.params.RollBackParam;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class RollBackOperator extends TransactionOperator {
    public static final RollBackOperator INSTANCE = new RollBackOperator();

    private RollBackOperator() {
    }

    @Override
    public boolean push(int pin, @Nullable Object[] tuple, Vertex vertex) {
        // key.add();
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        if (!(fin instanceof FinWithException)) {
            RollBackParam param = vertex.getParam();
            // 1、call sdk TxnRollBack
            TxnBatchRollBack rollBackRequest = TxnBatchRollBack.builder().
                isolationLevel(IsolationLevel.of(param.getIsolationLevel()))
                    .startTs(param.getStart_ts())
                    .keys(param.getKey())
                    .build();
            StoreInstance store = Services.KV_STORE.getInstance(new CommonId(CommonId.CommonType.TABLE, 2, 74438), new CommonId(CommonId.CommonType.DISTRIBUTION, 74440, 86127));
            boolean result = store.txnBatchRollback(rollBackRequest);
            vertex.getSoleEdge().transformToNext(new Object[]{result});
        }
        vertex.getSoleEdge().fin(fin);
    }

}
