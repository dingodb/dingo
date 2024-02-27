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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.meta.entity.Table;
import lombok.Getter;

import java.util.List;

@Getter
@JsonTypeName("txn_update")
@JsonPropertyOrder({"pessimisticTxn", "isolationLevel", "primaryLockKey", "lockTimeOut", "startTs", "forUpdateTs", "table", "schema", "keyMapping", "mapping", "updates", "hasAutoInc", "autoIncColIdx"})
public class TxnPartUpdateParam extends TxnPartModifyParam {

    @JsonProperty("mapping")
    private final TupleMapping mapping;
    @JsonProperty("updates")
    private final List<SqlExpr> updates;

    @JsonProperty("hasAutoInc")
    private final boolean hasAutoInc;

    @JsonProperty("autoIncColIdx")
    private final int autoIncColIdx;

    public TxnPartUpdateParam(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("mapping") TupleMapping mapping,
        @JsonProperty("updates") List<SqlExpr> updates,
        @JsonProperty("pessimisticTxn") boolean pessimisticTxn,
        @JsonProperty("isolationLevel") int isolationLevel,
        @JsonProperty("primaryLockKey") byte[] primaryLockKey,
        @JsonProperty("startTs") long startTs,
        @JsonProperty("forUpdateTs") long forUpdateTs,
        @JsonProperty("lockTimeOut") long lockTimeOut,
        Table table,
        @JsonProperty("hasAutoInc") boolean hasAutoInc,
        @JsonProperty("autoIncColIdx") int autoIncColIdx
    ) {
        super(tableId, schema, keyMapping, table, pessimisticTxn,
            isolationLevel, primaryLockKey, startTs, forUpdateTs, lockTimeOut);
        this.mapping = mapping;
        this.updates = updates;
        this.hasAutoInc = hasAutoInc;
        this.autoIncColIdx = autoIncColIdx;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        updates.forEach(expr -> expr.compileIn(schema, vertex.getParasType()));
    }

    public void inc() {
        count++;
    }

    @Override
    public void setParas(Object[] paras) {
        super.setParas(paras);
        updates.forEach(e -> e.setParas(paras));
    }
}
