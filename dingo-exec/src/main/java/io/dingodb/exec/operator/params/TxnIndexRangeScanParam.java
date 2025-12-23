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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
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
import io.dingodb.expr.rel.op.UngroupedAggregateOp;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.NullaryAggExpr;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.dingodb.common.mysql.scope.ScopeVariables.showCoprocessorExpr;

@Slf4j
@Getter
public class TxnIndexRangeScanParam extends ScanWithRelOpParam {

    @JsonProperty("indexSchema")
    private final DingoType indexSchema;

    @JsonProperty("indexTableId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId indexTableId;
    @JsonProperty("isLookup")
    private final boolean isLookup;
    @JsonProperty("isUnique")
    private final boolean isUnique;
    @JsonProperty("indexDefinition")
    protected final Table index;
    @JsonProperty("tableDefinition")
    protected final Table table;
    private final KeyValueCodec codec;
    private transient KeyValueCodec lookupCodec;
    @JsonProperty("scanTs")
    private final long scanTs;
    private final long timeout;
    @JsonProperty("mapList")
    protected List<Integer> mapList;
    @JsonProperty("selection")
    private TupleMapping selection2;
    @JsonProperty("isAutoCommit")
    private final boolean isAutoCommit;

    private RelOp otherRelOp;

    public TxnIndexRangeScanParam(CommonId indexTableId,
                                 CommonId tableId,
                                 TupleMapping keyMapping,
                                 DingoType outputSchema,
                                 boolean isUnique,
                                 Table index,
                                 Table table,
                                 boolean isLookup,
                                 long scanTs,
                                 long timeout,
                                 RelOp relOp,
                                 boolean pushDown,
                                 TupleMapping selection2,
                                 int limit,
                                  boolean isAutoCommit,
                                  RelOp otherRelOp) {
        super(tableId, index.tupleType(), keyMapping, relOp, outputSchema,
            pushDown, index.getVersion(), limit, table.getCodecVersion(), selection2.stream().boxed().collect(Collectors.toList()));
        this.indexSchema = index.tupleType();
        this.indexTableId = indexTableId;
        this.isLookup = isLookup;
        this.isUnique = isUnique;
        this.index = index;
        this.table = table;
        this.scanTs = scanTs;
        this.timeout = timeout;
        this.selection2 = selection2;
        this.isAutoCommit = isAutoCommit;
        this.codec = CodecService.getDefault().createKeyValueCodec(
            index.getCodecVersion(), index.version, index.tupleType(), index.keyMapping());
        if (isLookup) {
            lookupCodec = CodecService.getDefault().createKeyValueCodec(
                table.getCodecVersion(), table.version, table.tupleType(), table.keyMapping());
        } else {
            this.mapList = table.getColumnIndices2(index.getColumns());
        }
        this.otherRelOp = otherRelOp;
    }

    @Override
    public void init(Vertex vertex) {
        if (relOp == null) {
            return;
        }
        RelOp relOpCompile = relOp.compile(new DingoCompileContext(
            (TupleType) indexSchema.getType(),
            (TupleType) vertex.getParasType().getType()
        ), config);
        if (otherRelOp != null) {
            try {
                otherRelOp = otherRelOp.compile(new DingoCompileContext(
                    (TupleType) table.tupleType().getType(),
                    (TupleType) vertex.getParasType().getType()
                ), config);
            } catch (Exception e) {
                LogUtils.error(log, "otherRelOp {} compile error: {}", otherRelOp, e.getMessage(), e);
            }
        }
        if (pushDown) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            if (RelOpCoder.INSTANCE.visit(relOpCompile, os) == CodingFlag.OK) {
                List<Integer> selection = IntStream.range(0, indexSchema.fieldCount())
                    .boxed()
                    .collect(Collectors.toList());
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
                        selection = IntStream.range(0, indexSchema.fieldCount())
                            .boxed()
                            .collect(Collectors.toList());
                    }
                } else {
                    LogUtils.debug(log, "disable selection, jobId:{}, origin relOp: {}", vertex.getTask().getJobId(), relOp);
                }
                if (isSelection) {
                    relOpCompile = relOpCompile.compile(new DingoCompileContext(
                        (TupleType) indexSchema.select(TupleMapping.of(selection)).getType(),
                        (TupleType) vertex.getParasType().getType()
                    ), config);
                    os = new ByteArrayOutputStream();
                    if (RelOpCoder.INSTANCE.visit(relOpCompile, os) != CodingFlag.OK) {
                        relOp = relOpCompile;
                        return;
                    }
                }
                boolean forAggCount = false;
                if(relOpCompile instanceof UngroupedAggregateOp) {
                    if(((UngroupedAggregateOp) relOpCompile).getAggList().size() == 1) {
                        Expr expr = ((UngroupedAggregateOp) relOpCompile).getAggList().get(0);
                        if(expr instanceof NullaryAggExpr) {
                            if((((NullaryAggExpr)expr).getOp()).getName().equals("COUNT")) {
                                forAggCount = true;
                            }
                        }
                    }
                }
                TupleMapping keyMapping = indexKeyMapping();
                TupleMapping outputKeyMapping = TupleMapping.of(new int[]{});
                coprocessor = CoprocessorV2.builder()
                    .originalSchema(SchemaWrapperUtils.buildSchemaWrapper(indexSchema, keyMapping, indexTableId.seq))
                    .resultSchema(SchemaWrapperUtils.buildSchemaWrapper(indexSchema, outputKeyMapping, indexTableId.seq))
                    .selection(selection)
                    .relExpr(os.toByteArray())
                    .forAggCount(forAggCount)
                    .codecVersion(this.codecVersion)
                    .build();

                if(showCoprocessorExpr()) {
                    StringBuffer sb = new StringBuffer();
                    for(byte b : coprocessor.getRelExpr()) {
                        sb.append(String.format("%02X",b));
                    }
                    LogUtils.info(log, "Pushing down expression in IndexScan {} via coprocessor.", sb.toString());
                }
            }
        }
        relOp = relOpCompile;
    }

    public TupleMapping indexKeyMapping() {
        int[] mappings = new int[indexSchema.fieldCount()];
        int keyCount = 0;
        int colSize = indexSchema.fieldCount();
        for (int i = 0; i < colSize; i++) {
            int primaryKeyIndex = index.getColumns().get(i).primaryKeyIndex;
            if (primaryKeyIndex >= 0) {
                mappings[primaryKeyIndex] = i;
                keyCount++;
            }
        }
        return TupleMapping.of(Arrays.copyOf(mappings, keyCount));
    }

    public KeyValueCodec getPushDownCodec() {
        TupleMapping outputKeyMapping = TupleMapping.of(new int[]{});
        return CodecService.getDefault().createKeyValueCodec(
            index.getCodecVersion(), schemaVersion, indexSchema, outputKeyMapping);
    }

}
