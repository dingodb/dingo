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
import io.dingodb.exec.transaction.params.CommitParam;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class CommitOperator extends TransactionOperator {
    public static final CommitOperator INSTANCE = new CommitOperator();

    private CommitOperator() {
    }

    @Override
    public boolean push(int pin, @Nullable Object[] tuple, Vertex vertex) {
        // key.add();
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        CommitParam param = vertex.getParam();
        if (!(fin instanceof FinWithException)) {
            // 1、Async call sdk TxnCommit
            TxnCommit commitRequest = TxnCommit.builder().
                isolationLevel(IsolationLevel.of(param.getIsolationLevel())).
                start_ts(param.getStart_ts()).
                commit_ts(param.getCommit_ts()).
                keys(param.getKey()).
                build();
            // TODO
            StoreInstance store = Services.KV_STORE.getInstance(new CommonId(CommonId.CommonType.TABLE, 2, 74438), new CommonId(CommonId.CommonType.DISTRIBUTION, 74440, 86127));
            boolean result = store.txnCommit(commitRequest);
            vertex.getSoleEdge().transformToNext(new Object[]{result});
        }
        vertex.getSoleEdge().fin(fin);
    }

}
