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
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@JsonTypeName("commit")
public class CommitOperator extends TransactionOperator {
    @JsonProperty("isolationLevel")
    @Getter
    @Setter
    private int isolationLevel = 2;
    @JsonProperty("start_ts")
    @Getter
    @Setter
    private long start_ts;
    @JsonProperty("commit_ts")
    @Getter
    @Setter
    private long commit_ts;
    @JsonProperty("primaryKey")
    @Getter
    @Setter
    private byte[] primaryKey;
    private List<byte[]> key;

    @JsonCreator
    public CommitOperator(
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("isolationLevel") int isolationLevel,
        @JsonProperty("start_ts") long start_ts,
        @JsonProperty("commit_ts") long commit_ts,
        @JsonProperty("primaryKey") byte[] primaryKey) {
        super(schema);
        this.isolationLevel = isolationLevel;
        this.start_ts = start_ts;
        this.commit_ts = commit_ts;
        this.primaryKey = primaryKey;
    }

    @Override
    public void init() {
        super.init();
        key = new ArrayList<>();
    }

    @Override
    public boolean push(int pin, @Nullable Object[] tuple) {
        // key.add();
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin) {
        if (!(fin instanceof FinWithException)) {
            // 1、Async call sdk TxnCommit
            TxnCommit commitRequest = TxnCommit.builder().
                isolationLevel(IsolationLevel.of(isolationLevel)).
                start_ts(start_ts).
                commit_ts(commit_ts).
                keys(key).
                build();
            boolean result = part.txnCommit(commitRequest);
            output.push(new Object[]{result});
        }
        output.fin(fin);
    }

}
