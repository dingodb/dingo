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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.transaction.base.TransactionConfig;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@JsonTypeName("preWrite")
public final class PreWriteOperator extends TransactionOperator {

    @JsonProperty("primaryKey")
    @Getter
    @Setter
    private byte[] primaryKey;
    @Getter
    @Setter
    private Set<Mutation> mutations;
    @JsonProperty("start_ts")
    @Getter
    @Setter
    private long start_ts;
    @JsonProperty("lock_ttl")
    @Getter
    @Setter
    private long lock_ttl;
    @JsonProperty("isolationLevel")
    @Getter
    @Setter
    private int isolationLevel = 2;
    private boolean try_one_pc = false;
    private long max_commit_ts = 0l;


    @JsonCreator
    public PreWriteOperator(
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("primaryKey") byte[] primaryKey,
        @JsonProperty("start_ts") long start_ts,
        @JsonProperty("lock_ttl") long lock_ttl,
        @JsonProperty("isolationLevel") int isolationLevel) {
        super(schema);
        this.primaryKey = primaryKey;
        this.start_ts = start_ts;
        this.lock_ttl = lock_ttl;
        this.isolationLevel = isolationLevel;
    }

    @Override
    public void init() {
        super.init();
        mutations = new HashSet<>();
    }

    @Override
    public boolean push(int pin, @Nullable Object[] tuple) {

        // cache to mutations
        mutations.add(TransactionCacheToMutation.cacheToMutation(tuple));
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin) {
        if (!(fin instanceof FinWithException)) {
            // 1、call sdk TxnPreWrite
            List<List<Mutation>> mutationList = Lists.partition(new ArrayList<Mutation>(mutations), TransactionConfig.max_pre_write_count);
            txn_size = mutations.size();
            for (List<Mutation> subList : mutationList) {
                TxnPreWrite txnPreWrite = TxnPreWrite.builder().
                    isolationLevel(IsolationLevel.of(
                        isolationLevel
                    )).
                    mutations(subList).
                    primary_lock(primaryKey).
                    start_ts(start_ts).
                    lock_ttl(lock_ttl).
                    txn_size(txn_size).
                    try_one_pc(try_one_pc).
                    max_commit_ts(max_commit_ts).
                    build();
                boolean result = part.txnPreWrite(txnPreWrite);
                if (!result) {
                    break;
                }
            }
            output.push(new Object[]{true});
        }
        output.fin(fin);
    }

    @Override
    public DingoType getParasType() {
        return super.getParasType();
    }
}
