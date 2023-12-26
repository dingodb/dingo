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

package io.dingodb.exec.transaction.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.AbstractParams;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.store.api.transaction.data.Mutation;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@JsonTypeName("preWrite")
public class PreWriteParam extends AbstractParams {

    @JsonProperty("schema")
    private final DingoType schema;
    @JsonProperty("primaryKey")
    private final byte[] primaryKey;
    @JsonProperty("start_ts")
    private final long start_ts;
    @JsonProperty("isolationLevel")
    private final int isolationLevel;
    @JsonProperty("txnType")
    private final TransactionType transactionType;
    @Setter
    private List<Mutation> mutations;
    @Setter
    private long txn_size;
    @Setter
    private CommonId tableId;
    @Setter
    private CommonId partId;
    private boolean try_one_pc = false;
    private long max_commit_ts = 0l;

    public PreWriteParam(
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("primaryKey") byte[] primaryKey,
        @JsonProperty("start_ts") long start_ts,
        @JsonProperty("isolationLevel") int isolationLevel,
        @JsonProperty("txnType") TransactionType transactionType
    ) {
        this.schema = schema;
        this.primaryKey = primaryKey;
        this.start_ts = start_ts;
        this.isolationLevel = isolationLevel;
        this.transactionType = transactionType;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        mutations = new ArrayList<>();
    }

    public void addMutation(Mutation mutation) {
        mutations.add(mutation);
    }
}
