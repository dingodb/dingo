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
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.AbstractParams;
import io.dingodb.exec.operator.params.SourceParam;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.impl.TransactionCache;
import lombok.Getter;

@Getter
@JsonTypeName("scanCache")
@JsonPropertyOrder({"txnType", "schema"})
public class ScanCacheParam extends SourceParam {

    @JsonProperty("schema")
    protected final DingoType schema;
    @JsonProperty("txnType")
    protected final TransactionType transactionType;
    protected TransactionCache cache;

    public ScanCacheParam(
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("txnType") TransactionType transactionType
    ) {
        this.schema = schema;
        this.transactionType = transactionType;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        cache = new TransactionCache(vertex.getTask().getTxnId());
    }
}
