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
import io.dingodb.exec.expr.DingoCompileContext;
import io.dingodb.exec.expr.DingoRelConfig;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.expr.common.type.TupleType;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.meta.entity.Table;
import lombok.Getter;

import java.util.List;
import java.util.Objects;

@Getter
@JsonTypeName("pessimistic_lock")
@JsonPropertyOrder({"isolationLevel", "startTs", "forUpdateTs", "lockTimeOut",
    "pessimisticTxn", "isInsert", "table", "schema", "keyMapping", "isReplaceInto", "isIgnore",
    "updatePrimaryKey", "updateLimit"})
public class PessimisticLockParam extends TxnPartModifyParam {

    @JsonProperty("isInsert")
    protected final boolean isInsert;
    @JsonProperty("isScan")
    private final boolean isScan;
    @JsonProperty("opType")
    private final String opType;
    @JsonProperty("isDuplicateKeyUpdate")
    private final boolean isDuplicateUpdate;
    private boolean forUpdate;
    @JsonProperty("isReplaceInto")
    private final boolean isReplaceInto;
    @JsonProperty("isIgnore")
    private final boolean isIgnore;
    @JsonProperty("updatePrimaryKey")
    private final boolean updatePrimaryKey;
    @JsonProperty("mapping")
    private final TupleMapping mapping;
    @JsonProperty("updates")
    private final List<SqlExpr> updates;
    @JsonProperty("updateLimit")
    private final long updateLimit;

    private long updateScanCount;

    private RelOp relOp;
    private final DingoRelConfig config;

    public PessimisticLockParam(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("isolationLevel") int isolationLevel,
        @JsonProperty("startTs") long startTs,
        @JsonProperty("forUpdateTs") long forUpdateTs,
        @JsonProperty("pessimisticTxn") boolean pessimisticTxn,
        @JsonProperty("primaryLockKey") byte[] primaryLockKey,
        @JsonProperty("lockTimeOut") long lockTimeOut,
        @JsonProperty("isInsert") boolean isInsert,
        @JsonProperty("isScan") boolean isScan,
        @JsonProperty("opType") String opType,
        Table table,
        boolean isDuplicateUpdate,
        boolean forUpdate,
        @JsonProperty("isReplaceInto") boolean isReplaceInto,
        @JsonProperty("isIgnore") boolean isIgnore,
        @JsonProperty("updatePrimaryKey") boolean updatePrimaryKey,
        @JsonProperty("mapping") TupleMapping mapping,
        @JsonProperty("updates") List<SqlExpr> updates,
        @JsonProperty("updateLimit") long updateLimit,
        RelOp relOp
    ) {
        super(tableId, schema, keyMapping, table, pessimisticTxn,
            isolationLevel, primaryLockKey, startTs, forUpdateTs, lockTimeOut);
        this.isInsert = isInsert;
        this.isScan = isScan;
        this.opType = opType;
        this.isDuplicateUpdate = isDuplicateUpdate;
        this.forUpdate = forUpdate;
        this.isReplaceInto = isReplaceInto;
        this.isIgnore = isIgnore;
        this.updatePrimaryKey = updatePrimaryKey;
        this.mapping = mapping;
        this.updates = updates;
        this.updateLimit = updateLimit;
        this.updateScanCount = 0L;
        this.relOp = relOp;
        this.config = new DingoRelConfig();
    }
    public void inc() {
        count++;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        if (updates != null && !updates.isEmpty() && relOp == null) {
            updates.stream().filter(Objects::nonNull).forEach(expr -> expr.compileIn(schema, vertex.getParasType()));
        }
        if (relOp != null) {
            relOp = relOp.compile(new DingoCompileContext(
                (TupleType) schema.getType(),
                (TupleType) vertex.getParasType().getType()
            ), config);
        }
    }

    public void incUpdateScanCount() {
        updateScanCount++;
    }

    @Override
    public void setParas(Object[] paras) {
        super.setParas(paras);
        if (updates != null && !updates.isEmpty()) {
            updates.stream().filter(Objects::nonNull).forEach(e -> e.setParas(paras));
        }
        if (relOp != null) {
            config.getEvalContext().setParas(paras);
        }
    }
}
