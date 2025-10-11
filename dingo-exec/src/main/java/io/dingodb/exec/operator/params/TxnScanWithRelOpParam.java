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
import io.dingodb.common.CoprocessorV2;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.DingoCompileContext;
import io.dingodb.exec.utils.SchemaWrapperUtils;
import io.dingodb.exec.utils.relop.RelOpMappingVisitor;
import io.dingodb.exec.utils.relop.RelOpSelectionVisitor;
import io.dingodb.exec.utils.relop.SelectionFlag;
import io.dingodb.exec.utils.relop.SelectionObj;
import io.dingodb.expr.coding.CodingFlag;
import io.dingodb.expr.coding.RelOpCoder;
import io.dingodb.expr.common.type.TupleType;
import io.dingodb.expr.rel.RelOp;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayOutputStream;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Getter
@JsonTypeName("txnScanRel")
@JsonPropertyOrder({
    "tableId",
    "schema",
    "keyMapping",
    "outputSchema",
    "scanTs",
    "isolationLevel",
    "timeOut",
    "rel",
    "pushDown",
})
public class TxnScanWithRelOpParam extends ScanWithRelOpParam {
    @JsonProperty("isolationLevel")
    private final int isolationLevel;
    @JsonProperty("timeOut")
    private final long timeOut;
    @JsonProperty("scanTs")
    private long scanTs;
    private final boolean isAutoCommit;

    public TxnScanWithRelOpParam(
        CommonId tableId,
        @NonNull DingoType schema,
        TupleMapping keyMapping,
        long scanTs,
        int isolationLevel,
        long timeOut,
        @NonNull RelOp relOp,
        DingoType outputSchema,
        boolean pushDown,
        int schemaVersion,
        int limit,
        int codecVersion,
        List<Integer> selection,
        boolean isAutoCommit
    ) {
        super(tableId, schema, keyMapping, relOp, outputSchema, pushDown, schemaVersion, limit, codecVersion, selection);
        this.scanTs = scanTs;
        this.isolationLevel = isolationLevel;
        this.timeOut = timeOut;
        this.isAutoCommit = isAutoCommit;
    }

    @Override
    public void setStartTs(long startTs) {
        super.setStartTs(startTs);
        this.scanTs = startTs;
    }

    @Override
    public void init(Vertex vertex) {
        if (selection == null) {
            selection = IntStream.range(0, schema.fieldCount())
                .boxed()
                .collect(Collectors.toList());
        }
        RelOp relOpCompile = relOp.compile(new DingoCompileContext(
            (TupleType) schema.getType(),
            (TupleType) vertex.getParasType().getType()
        ), config);
        if (pushDown) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            if (RelOpCoder.INSTANCE.visit(relOpCompile, os) == CodingFlag.OK) {
                Set<Integer> selections = new HashSet<>();
                SelectionObj selectionObj = new SelectionObj(selections, true, 0);
                boolean isSelection = false;
                if (isAutoCommit() && RelOpSelectionVisitor.INSTANCE.visit(relOp, selectionObj) == SelectionFlag.OK
                    && selectionObj.isProject() && selections.size() != selection.size() && !selections.isEmpty()) {
                    try {
                        selection.clear();
                        selection.addAll(selections);
                        selection.sort(Comparator.naturalOrder());
                        relOpCompile = RelOpMappingVisitor.INSTANCE.visit(relOp, selection);
                        LogUtils.debug(log, "jobId:{}, new relOp: {}", vertex.getTask().getJobId(), relOpCompile);
                        isSelection = true;
                    } catch (Exception e) {
                        LogUtils.error(log, "relOp failed via selection, " + e.getMessage(), e);
                        selection = IntStream.range(0, schema.fieldCount())
                            .boxed()
                            .collect(Collectors.toList());
                    }
                } else {
                    LogUtils.debug(log, "disable selection, jobId:{}, origin relOp: {}", vertex.getTask().getJobId(), relOp);
                }
                if (isSelection) {
                    relOpCompile = relOpCompile.compile(new DingoCompileContext(
                        (TupleType) schema.select(TupleMapping.of(selection)).getType(),
                        (TupleType) vertex.getParasType().getType()
                    ), config);
                    os = new ByteArrayOutputStream();
                    if (RelOpCoder.INSTANCE.visit(relOpCompile, os) != CodingFlag.OK) {
                        relOp = relOpCompile;
                        return;
                    }
                }
                TupleMapping outputKeyMapping = TupleMapping.of(new int[]{});
                coprocessor = CoprocessorV2.builder()
                    .originalSchema(SchemaWrapperUtils.buildSchemaWrapper(schema, keyMapping, tableId.seq))
                    .resultSchema(SchemaWrapperUtils.buildSchemaWrapper(outputSchema, outputKeyMapping, tableId.seq))
                    .selection(selection)
                    .relExpr(os.toByteArray())
                    .codecVersion(codecVersion)
                    .build();
                if (limit > 0) {
                    coprocessor.setLimit(limit);
                }
            }
        }
        relOp = relOpCompile;
    }

}
