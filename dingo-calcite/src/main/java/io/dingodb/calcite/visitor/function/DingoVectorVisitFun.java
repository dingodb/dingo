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

package io.dingodb.calcite.visitor.function;

import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoVector;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.ListType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DecimalType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.fun.vector.VectorImageFun;
import io.dingodb.exec.fun.vector.VectorTextFun;
import io.dingodb.exec.operator.params.PartVectorParam;
import io.dingodb.exec.operator.params.TxnPartVectorParam;
import io.dingodb.exec.restful.VectorExtract;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import io.dingodb.serial.schema.IntegerSchema;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rel.LogicalDingoTableScan.getIndexMetricType;
import static io.dingodb.common.util.Utils.isNeedLookUp;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_VECTOR;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_VECTOR;

@Slf4j
public final class DingoVectorVisitFun {

    // tmp use
    public static List<Object> pushDownSchemaList = new ArrayList<>();

    static {
        pushDownSchemaList.add(IntegerType.class);
        pushDownSchemaList.add(LongType.class);
        pushDownSchemaList.add(BooleanType.class);
        pushDownSchemaList.add(FloatType.class);
        pushDownSchemaList.add(DoubleType.class);
        pushDownSchemaList.add(DecimalType.class);
        pushDownSchemaList.add(StringType.class);
    }

    private DingoVectorVisitFun() {

    }

    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation,
        ITransaction transaction, DingoJobVisitor visitor, DingoVector rel
    ) {
        DingoRelOptTable relTable = rel.getTable();
        DingoTable dingoTable = relTable.unwrap(DingoTable.class);

        MetaService metaService = MetaService.root().getSubMetaService(relTable.getSchemaName());
        assert dingoTable != null;
        CommonId tableId = dingoTable.getTableId();
        Table td = dingoTable.getTable();

        NavigableMap<ComparableByteArray, RangeDistribution> ranges = metaService.getRangeDistribution(tableId);
        List<Object> operandsList = rel.getOperands();

        SqlIdentifier vectorColNmIdf = (SqlIdentifier) operandsList.get(1);
        String vectorColNm = "";
        if (vectorColNmIdf != null) {
            vectorColNm = vectorColNmIdf.getSimple();
        }
        Float[] floatArray = getVectorFloats(operandsList);

        if (!(operandsList.get(3) instanceof SqlNumericLiteral)) {
            throw new IllegalArgumentException("Top n not number.");
        }

        int topN = ((Number) Objects.requireNonNull(((SqlNumericLiteral) operandsList.get(3)).getValue())).intValue();

        List<Vertex> outputs = new ArrayList<>();

        IndexTable indexTable = (IndexTable) rel.getIndexTable();
        boolean pushDown = pushDown(rel.getFilter(), dingoTable.getTable(), indexTable);
        RexNode rexFilter = rel.getFilter();
        TupleMapping resultSelection = rel.getSelection();

        List<Column> columnNames = indexTable.getColumns();
        List<Integer> priKeySecList = dingoTable.getTable()
            .columns.stream()
            .filter(Column::isPrimary)
            .map(dingoTable.getTable().columns::indexOf)
            .collect(Collectors.toList());
        int priKeyCount = priKeySecList.size();
        // vector index cols in pri table selection
        List<Integer> indexLookupSelectionList = columnNames
            .stream()
            .filter(col -> !col.isPrimary() && col.getState() == 1)
            .map(dingoTable.getTable().columns::indexOf)
            .collect(Collectors.toList());
        priKeySecList.addAll(indexLookupSelectionList);
        boolean isLookUp = isNeedLookUp(
            resultSelection,
            TupleMapping.of(priKeySecList),
            dingoTable.getTable().columns.size()
        );
        boolean isTxn = dingoTable.getTable().getEngine().contains("TXN");
        TupleMapping pushDownSelection;
        if (pushDown && isTxn) {
            List<Integer> secList = new ArrayList<>();
            for (int i = 0; i < priKeyCount; i ++) {
                secList.add(i);
            }
            AtomicInteger ix = new AtomicInteger(priKeyCount);
            List<Integer> indexList = indexTable
                .getColumns()
                .stream()
                .filter(col -> col.getState() == 1 && !col.isPrimary())
                .map(col -> ix.getAndIncrement())
                .collect(Collectors.toList());
            secList.addAll(indexList);
            pushDownSelection = TupleMapping.of(secList);

            Mapping mapping = Mappings.target(priKeySecList, dingoTable.getTable().getColumns().size());
            rexFilter = RexUtil.apply(mapping, rexFilter);
        } else {
            TupleMapping realSelection = rel.getRealSelection();
            List<Integer> selectedColumns = realSelection.stream().boxed().collect(Collectors.toList());
            Mapping mapping = Mappings.target(selectedColumns, dingoTable.getTable().getColumns().size() + 1);
            rexFilter = (rexFilter != null) ? RexUtil.apply(mapping, rexFilter) : null;
            pushDownSelection = resultSelection;
        }
        SqlExpr filter = null;
        if (rexFilter != null) {
            filter = SqlExprUtils.toSqlExpr(rexFilter);
        }
        long scanTs = Optional.ofNullable(transaction).map(ITransaction::getStartTs).orElse(0L);
        // Use current read
        if (transaction != null && transaction.isPessimistic()
            && IsolationLevel.of(transaction.getIsolationLevel()) == IsolationLevel.SnapshotIsolation
            && (visitor.getKind() == SqlKind.INSERT || visitor.getKind() == SqlKind.DELETE
            || visitor.getKind() == SqlKind.UPDATE) ) {
            scanTs = TsoService.getDefault().tso();
        }
        if (transaction != null && transaction.isPessimistic()
            && IsolationLevel.of(transaction.getIsolationLevel()) == IsolationLevel.ReadCommitted
            && visitor.getKind() == SqlKind.SELECT) {
            scanTs = TsoService.getDefault().tso();
        }

        // Get query additional parameters
        Map<String, Object> parameterMap = getParameterMap(operandsList);
        // Get all index table distributions
        NavigableMap<ComparableByteArray, RangeDistribution> indexRanges =
            metaService.getRangeDistribution(rel.getIndexTableId());

        // Create tasks based on partitions
        for (RangeDistribution rangeDistribution : indexRanges.values()) {
            Vertex vertex;
            if (transaction == null) {
                PartVectorParam param = new PartVectorParam(
                    tableId,
                    rangeDistribution.id(),
                    rel.tupleType(),
                    td.keyMapping(),
                    Optional.mapOrNull(filter, SqlExpr::copy),
                    rel.getSelection(),
                    td,
                    ranges,
                    rel.getIndexTableId(),
                    floatArray,
                    topN,
                    parameterMap
                );
                vertex = new Vertex(PART_VECTOR, param);
            } else {
                String finalVectorColNm = vectorColNm;
                int vectorIdx = dingoTable.getTable()
                    .columns
                    .stream()
                    .filter(col -> col.getName().equalsIgnoreCase(finalVectorColNm))
                    .map(col -> dingoTable.getTable().columns.indexOf(col))
                    .findFirst().orElse(10000);
                String metricType = getIndexMetricType(dingoTable, vectorColNm);

                RelOp relOp = null;
                if (rexFilter != null) {
                    Expr expr = RexConverter.convert(rexFilter);
                    relOp = RelOpBuilder.builder()
                        .filter(expr)
                        .build();
                }
                TxnPartVectorParam param = new TxnPartVectorParam(
                    rangeDistribution.id(),
                    Optional.mapOrNull(filter, SqlExpr::copy),
                    pushDownSelection,
                    rel.tupleType(),
                    td,
                    ranges,
                    floatArray,
                    topN,
                    parameterMap,
                    indexTable,
                    relOp,
                    pushDown,
                    isLookUp,
                    scanTs,
                    transaction.getIsolationLevel(),
                    transaction.getLockTimeOut(),
                    resultSelection,
                    vectorIdx,
                    metricType
                );
                vertex = new Vertex(TXN_PART_VECTOR, param);
            }
            Task task = job.getOrCreate(currentLocation, idGenerator);
            OutputHint hint = new OutputHint();
            hint.setPartId(rangeDistribution.id());
            vertex.setHint(hint);
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            outputs.add(vertex);
        }

        return outputs;
    }

    public static Float[] getVectorFloats(List<Object> operandsList) {
        Float[] floatArray = null;
        Object call = operandsList.get(2);
        if (call instanceof RexCall) {
            RexCall rexCall = (RexCall) call;
            floatArray = new Float[rexCall.getOperands().size()];
            int vectorDimension = rexCall.getOperands().size();
            for (int i = 0; i < vectorDimension; i++) {
                RexLiteral literal = (RexLiteral) rexCall.getOperands().get(i);
                floatArray[i] = literal.getValueAs(Float.class);
            }
            return floatArray;
        }
        SqlBasicCall basicCall = (SqlBasicCall) operandsList.get(2);
        if (basicCall.getOperator() instanceof SqlArrayValueConstructor) {
            List<SqlNode> operands = basicCall.getOperandList();
            floatArray = new Float[operands.size()];
            for (int i = 0; i < operands.size(); i++) {
                floatArray[i] = (
                    (Number) Objects.requireNonNull(((SqlNumericLiteral) operands.get(i)).getValue())
                ).floatValue();
            }
        } else {
            List<SqlNode> sqlNodes = basicCall.getOperandList();
            if (sqlNodes.size() < 2) {
                throw new RuntimeException("vector load param error");
            }
            List<Object> paramList = sqlNodes.stream().map(e -> {
                if (e instanceof SqlLiteral) {
                    return ((SqlLiteral)e).getValue();
                } else if (e instanceof SqlIdentifier) {
                    return ((SqlIdentifier)e).getSimple();
                } else {
                    return e.toString();
                }
            }).collect(Collectors.toList());
            if (paramList.get(1) == null || paramList.get(0) == null) {
                throw new RuntimeException("vector load param error");
            }
            String param = paramList.get(1).toString();
            if (param.contains("'")) {
                param = param.replace("'", "");
            }
            String funcName = basicCall.getOperator().getName();
            if (funcName.equalsIgnoreCase(VectorTextFun.NAME)) {
                floatArray = VectorExtract.getTxtVector(
                    basicCall.getOperator().getName(),
                    paramList.get(0).toString(),
                    param);
            } else if (funcName.equalsIgnoreCase(VectorImageFun.NAME)) {
                if (paramList.size() < 3) {
                    throw new RuntimeException("vector load param error");
                }
                Object localPath = paramList.get(2);
                if (!(localPath instanceof Boolean)) {
                    throw new RuntimeException("vector load param error");
                }
                floatArray = VectorExtract.getImgVector(
                    basicCall.getOperator().getName(),
                    paramList.get(0).toString(),
                    paramList.get(1),
                    (Boolean) paramList.get(2));
            }
        }
        if (floatArray == null) {
            throw new RuntimeException("vector load error");
        }
        return floatArray;
    }

    private static Map<String, Object> getParameterMap(List<Object> operandsList) {
        Map<String, Object> parameterMap = new HashMap<>();
        if (operandsList.size() >= 5) {
            SqlNode sqlNode = (SqlNode) operandsList.get(4);
            if (sqlNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) operandsList.get(4);
                if (sqlBasicCall.getOperator().getName().equals("MAP")) {
                    List<SqlNode> operandList = sqlBasicCall.getOperandList();
                    String currentName = "";
                    for (int i = 0; i < operandList.size(); i++) {
                        if ((i % 2 == 0) && operandList.get(i) instanceof SqlIdentifier) {
                            currentName = ((SqlIdentifier) operandList.get(i)).getSimple();
                        } else {
                            SqlNode node = operandList.get(i);
                            if (!currentName.equals("") && node instanceof SqlNumericLiteral) {
                                parameterMap.put(currentName, ((SqlNumericLiteral)node).getValue());
                            }
                        }
                    }
                }
            }
        }

        return parameterMap;
    }

    private static boolean pushDown(RexNode filter, Table table, IndexTable indexTable) {
        if (filter == null) {
            return false;
        }
        List<Integer> inputRefList = new ArrayList<>();
        RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(@NonNull RexInputRef inputRef) {
                if (!inputRefList.contains(inputRef.getIndex())) {
                    inputRefList.add(inputRef.getIndex());
                }
                return super.visitInputRef(inputRef);
            }
        };
        filter.accept(visitor);
        List<Column> selectionColList = inputRefList.stream()
            .map(i -> table.getColumns().get(i))
            .filter(col -> !col.isPrimary())
            .collect(Collectors.toList());
        if (selectionColList.size() == 0) {
            return false;
        }
        boolean recognizableTypes = selectionColList.stream()
            .allMatch(column -> pushDownSchemaList.contains(column.type.getClass()));
        if (!recognizableTypes) {
            return false;
        }

        // vector index only with field can push down
        List<Column> filterIndexCols = indexTable.getColumns().stream()
            .filter(col -> !col.isPrimary() && !(col.getType() instanceof ListType))
            .collect(Collectors.toList());
        java.util.Optional<Column> optional = selectionColList.stream()
            .filter(column -> !filterIndexCols.contains(column))
            .findFirst();
        return !optional.isPresent();
    }

}
