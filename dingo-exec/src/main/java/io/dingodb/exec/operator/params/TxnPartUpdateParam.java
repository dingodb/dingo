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
import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.DingoCompileContext;
import io.dingodb.exec.expr.DingoRelConfig;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.tuple.TupleKey;
import io.dingodb.expr.common.type.TupleType;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.ProjectOp;
import io.dingodb.expr.runtime.ExprContext;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import org.apache.calcite.rel.core.TableModify;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
@JsonTypeName("txn_update")
@JsonPropertyOrder({
    "pessimisticTxn",
    "isolationLevel",
    "primaryLockKey",
    "lockTimeOut",
    "startTs",
    "forUpdateTs",
    "table",
    "schema",
    "keyMapping",
    "mapping",
    "updates",
    "hasAutoInc",
    "autoIncColIdx",
    "updatePrimaryKey",
    "updateLimit"})
public class TxnPartUpdateParam extends TxnPartModifyParam {

    @JsonProperty("mapping")
    private final TupleMapping mapping;
    @JsonProperty("updates")
    private final List<SqlExpr> updates;

    @JsonProperty("hasAutoInc")
    private final boolean hasAutoInc;

    @JsonProperty("autoIncColIdx")
    private final int autoIncColIdx;

    @JsonProperty("updatePrimaryKey")
    private final boolean updatePrimaryKey;

    @JsonProperty("updateLimit")
    private final long updateLimit;

    private long updateScanCount;

    @JsonProperty("relOp")
    private RelOp relOp;
    public final DingoRelConfig config;

    private int indexSize;

    private transient Map<TupleKey, Integer> updateKeys;

    // multi-table update
    private CommonId joinTableId;
    private TableModify.TableInfo tableInfo;
    private List<String> targetTableNames;
    private boolean isLeft;

    private transient Map<TupleKey, TableIndex> tableIndexMap;

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
        @JsonProperty("autoIncColIdx") int autoIncColIdx,
        @JsonProperty("updatePrimaryKey") boolean updatePrimaryKey,
        @JsonProperty("updateLimit") long updateLimit,
        RelOp relOp,
        CommonId joinTableId,
        TableModify.TableInfo tableInfo,
        List<String> targetTableNames,
        boolean isLeft
    ) {
        super(tableId, schema, keyMapping, table, pessimisticTxn,
            isolationLevel, primaryLockKey, startTs, forUpdateTs, lockTimeOut);
        this.mapping = mapping;
        this.updates = updates;
        this.hasAutoInc = hasAutoInc;
        this.autoIncColIdx = autoIncColIdx;
        this.updatePrimaryKey =  updatePrimaryKey;
        this.updateLimit = updateLimit;
        this.updateScanCount = 0L;
        this.relOp = relOp;
        this.config = new DingoRelConfig();
        this.indexSize = 0;
        this.codec = CodecService.getDefault().createKeyValueCodec(
            table.getCodecVersion(), table.version, schema, table.keyMapping());
        this.joinTableId = joinTableId;
        this.tableInfo = tableInfo;
        this.targetTableNames = targetTableNames;
        this.isLeft = isLeft;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        // updates.forEach(expr -> expr.compileIn(schema, vertex.getParasType()));
        DingoCompileContext dingoCompileContext = new DingoCompileContext(
            (TupleType) schema.getType(),
            (TupleType) vertex.getParasType().getType()
        );

        if (this.relOp instanceof ProjectOp) {
            if (((ProjectOp)(this.relOp)).getExprConfig().getExprContext() == ExprContext.CALC_VALUE) {
                dingoCompileContext.setExprContext(io.dingodb.expr.runtime.ExprContext.CALC_VALUE);
            }
        }

        relOp = relOp.compile(dingoCompileContext, config);

        if (updateLimit != -1L) {
            indexSize = table.getIndexes().size();
            updateKeys = new ConcurrentHashMap<>();
        }
        if (!tableInfo.isSingleSource()) {
            updateKeys = new ConcurrentHashMap<>();
            tableIndexMap = new ConcurrentHashMap<>();
        }
    }

    public void inc() {
        count++;
    }

    public void incUpdateScanCount() {
        updateScanCount++;
    }

    @Override
    public void setParas(Object[] paras) {
        super.setParas(paras);
        // updates.forEach(e -> e.setParas(paras));
        config.getEvalContext().setParas(paras);
    }

    @Getter
    public static class TableIndex {
        public static AtomicInteger tableKeyCount = new AtomicInteger(-1);
        public static AtomicInteger indexKeyCount = new AtomicInteger(-1);

        public TableIndex(int tableCount, int indexCount) {
            tableKeyCount.addAndGet(tableCount);
            indexKeyCount.addAndGet(indexCount);
        }
    }
}
