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
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.operator.params.AbstractParams;
import io.dingodb.store.api.transaction.data.Mutation;
import lombok.Getter;
import lombok.Setter;

import java.util.Set;

@Getter
@JsonTypeName("preWrite")
public class PreWriteParam extends AbstractParams {

    @JsonProperty("schema")
    private final DingoType schema;
    @JsonProperty("primaryKey")
    @Setter
    private byte[] primaryKey;
    @Setter
    private Set<Mutation> mutations;
    @JsonProperty("start_ts")
    @Setter
    private long start_ts;
    @JsonProperty("lock_ttl")
    @Setter
    private long lock_ttl;
    @JsonProperty("isolationLevel")
    @Setter
    private int isolationLevel = 2;
    @Setter
    private long txn_size;
    private boolean try_one_pc = false;
    private long max_commit_ts = 0l;

    public PreWriteParam(
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("primaryKey") byte[] primaryKey,
        @JsonProperty("start_ts") long start_ts,
        @JsonProperty("lock_ttl") long lock_ttl,
        @JsonProperty("isolationLevel") int isolationLevel
    ) {
        this.schema = schema;
        this.primaryKey = primaryKey;
        this.start_ts = start_ts;
        this.lock_ttl = lock_ttl;
        this.isolationLevel = isolationLevel;
    }

    public void addMutation(Mutation mutation) {
        mutations.add(mutation);
    }
}
