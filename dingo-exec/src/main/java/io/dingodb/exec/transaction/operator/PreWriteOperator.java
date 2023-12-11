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

import com.google.common.collect.Lists;
import io.dingodb.common.CommonId;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.transaction.base.TransactionConfig;
import io.dingodb.exec.transaction.params.PreWriteParam;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public final class PreWriteOperator extends TransactionOperator {
    public static final PreWriteOperator INSTANCE = new PreWriteOperator();

    private PreWriteOperator() {
    }

    @Override
    public boolean push(int pin, @Nullable Object[] tuple, Vertex vertex) {
        PreWriteParam param = vertex.getParam();
        // cache to mutations
        param.addMutation(TransactionCacheToMutation.cacheToMutation(tuple));
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        if (!(fin instanceof FinWithException)) {
            PreWriteParam param = vertex.getParam();
            // 1、call sdk TxnPreWrite
            List<List<Mutation>> mutationList = Lists.partition(
                new ArrayList<Mutation>(param.getMutations()),
                TransactionConfig.max_pre_write_count);
            param.setTxn_size(param.getMutations().size());
            for (List<Mutation> subList : mutationList) {
                TxnPreWrite txnPreWrite = TxnPreWrite.builder().
                    isolationLevel(IsolationLevel.of(param.getIsolationLevel()))
                        .mutations(subList)
                        .primary_lock(param.getPrimaryKey())
                        .start_ts(param.getStart_ts())
                        .lock_ttl(param.getLock_ttl())
                        .txn_size(param.getTxn_size())
                        .try_one_pc(param.isTry_one_pc())
                        .max_commit_ts(param.getMax_commit_ts())
                        .build();
                // TODO
                StoreInstance store = Services.KV_STORE.getInstance(new CommonId(CommonId.CommonType.TABLE, 2, 74438), new CommonId(CommonId.CommonType.DISTRIBUTION, 74440, 86127));
                boolean result = store.txnPreWrite(txnPreWrite);
                if (!result) {
                    break;
                }
            }
            vertex.getSoleEdge().transformToNext(new Object[]{true});
        }
        vertex.getSoleEdge().fin(fin);
    }

}
