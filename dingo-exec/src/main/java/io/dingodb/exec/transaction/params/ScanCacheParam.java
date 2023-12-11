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
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.impl.TransactionCache;
import io.dingodb.exec.transaction.impl.TransactionManager;
import lombok.Getter;

@Getter
@JsonTypeName("scanCache")
public class ScanCacheParam extends AbstractParams {

    @JsonProperty("schema")
    private final DingoType schema;
    private TransactionCache cache;

    public ScanCacheParam(DingoType schema) {
        this.schema = schema;
    }

    public ScanCacheParam(DingoType schema, TransactionCache cache) {
        this.schema = schema;
        this.cache = cache;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        // cross node transaction
        if (cache == null) {
            CommonId txnId = vertex.getTask().getTxnId();
            ITransaction transaction = TransactionManager.getTransaction(txnId);
            cache = transaction.getCache();
        }
    }
}
