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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.AbstractParams;
import io.dingodb.exec.transaction.base.TransactionType;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@JsonTypeName("commit")
@JsonPropertyOrder({"txnType", "isolationLevel", "primaryKey", "startTs", "commitTs", "schema"})
public class CommitParam extends AbstractParams {

    @JsonProperty("schema")
    private final DingoType schema;
    @JsonProperty("isolationLevel")
    private final int isolationLevel;
    @JsonProperty("startTs")
    private final long startTs;
    @JsonProperty("commitTs")
    private final long commitTs;
    @JsonProperty("primaryKey")
    private final byte[] primaryKey;
    @JsonProperty("txnType")
    private final TransactionType transactionType;
    private List<byte[]> keys;
    @Setter
    private CommonId tableId;
    @Setter
    private CommonId partId;

    public CommitParam(
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("isolationLevel") int isolationLevel,
        @JsonProperty("startTs") long startTs,
        @JsonProperty("commitTs") long commitTs,
        @JsonProperty("primaryKey") byte[] primaryKey,
        @JsonProperty("txnType") TransactionType transactionType
    ) {
        this.schema = schema;
        this.isolationLevel = isolationLevel;
        this.startTs = startTs;
        this.commitTs = commitTs;
        this.primaryKey = primaryKey;
        this.transactionType = transactionType;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        keys = new ArrayList<>();
    }

    public void addKey(byte[] key) {
        keys.add(key);
    }
}
