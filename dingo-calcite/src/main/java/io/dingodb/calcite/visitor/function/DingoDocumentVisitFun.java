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
import io.dingodb.calcite.rel.DingoDocument;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.utils.VisitUtils;
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
import io.dingodb.exec.operator.params.PartDocumentParam;
import io.dingodb.exec.operator.params.TxnPartDocumentParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.dingodb.common.util.Utils.isNeedLookUp;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_DOCUMENT;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_DOCUMENT;

@Slf4j
public final class DingoDocumentVisitFun {

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

    private DingoDocumentVisitFun() {

    }

    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation,
        ITransaction transaction, DingoJobVisitor visitor, DingoDocument rel
    ) {
        DingoRelOptTable relTable = rel.getTable();
        DingoTable dingoTable = relTable.unwrap(DingoTable.class);

        assert dingoTable != null;

        List<Object> operandsList = rel.getOperands();
        String indexName = Objects.requireNonNull((SqlIdentifier) operandsList.get(1)).toString();
        String indexTableName = rel.getIndexTable().getName();
        if (!indexTableName.equalsIgnoreCase(indexName)) {
            throw new IllegalArgumentException("Can not find the text index with name: " + indexName);
        }
        IndexTable indexTable = (IndexTable) rel.getIndexTable();
        boolean pushDown = false;
        RexNode rexFilter = rel.getFilter();

        String queryString;
        if (rel.isDocumentScanFilter()) {
            queryString = Objects.requireNonNull((SqlCharStringLiteral) operandsList.get(2)).getStringValue();
        } else {
            queryString = rel.getQueryStr();
            rexFilter = null;
        }

        if (!(operandsList.get(3) instanceof SqlNumericLiteral)) {
            throw new IllegalArgumentException("Top n not a number.");
        }


        TupleMapping resultSelection = rel.getSelection();

        List<Column> columnNames = indexTable.getColumns();
        for (Column columnName : columnNames) {
            queryString = queryString.replaceAll(
                columnName.getName().toLowerCase() + ":",
                columnName.getName().toUpperCase() + ":"
            );
        }
        // document index cols in pri table selection
        TupleMapping map1 = indexTable.getMapping();
        List<Integer> indexLookupSelectionList = new ArrayList<>();
        for (int j = 0; j < map1.size(); j++) {
            indexLookupSelectionList.add(map1.get(j));
        }
        List<Integer> priKeySecList = dingoTable.getTable()
            .columns.stream()
            .filter(Column::isPrimary)
            .map(dingoTable.getTable().columns::indexOf)
            .collect(Collectors.toList());
        int priKeyCount = priKeySecList.size();

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
        long scanTs = VisitUtils.getScanTs(transaction, visitor.getKind(), visitor.getPointTs());

        // Get all index table distributions
        NavigableMap<ComparableByteArray, RangeDistribution> indexRanges =
            MetaService.root(visitor.getPointTs()).getRangeDistribution(rel.getIndexTableId());

        Table td = dingoTable.getTable();
        CommonId tableId = dingoTable.getTableId();
        NavigableMap<ComparableByteArray, RangeDistribution> ranges = MetaService.root().getRangeDistribution(tableId);
        int topN = ((Number) Objects.requireNonNull(((SqlNumericLiteral) operandsList.get(3)).getValue())).intValue();
        // Create tasks based on partitions
        List<Vertex> outputs = new ArrayList<>();
        for (RangeDistribution rangeDistribution : indexRanges.values()) {
            Vertex vertex;
            if (transaction == null) {
                PartDocumentParam param = new PartDocumentParam(
                    tableId,
                    rangeDistribution.id(),
                    rel.tupleType(),
                    td.keyMapping(),
                    Optional.mapOrNull(filter, SqlExpr::copy),
                    rel.getSelection(),
                    td,
                    ranges,
                    rel.getIndexTableId(),
                    queryString,
                    topN
                );
                vertex = new Vertex(PART_DOCUMENT, param);
            } else {

                RelOp relOp = null;
                if (rexFilter != null) {
                    Expr expr = RexConverter.convert(rexFilter);
                    relOp = RelOpBuilder.builder()
                        .filter(expr)
                        .build();
                }
                TxnPartDocumentParam param = new TxnPartDocumentParam(
                    rangeDistribution.id(),
                    Optional.mapOrNull(filter, SqlExpr::copy),
                    pushDownSelection,
                    rel.tupleType(),
                    td,
                    ranges,
                    queryString,
                    topN,
                    indexTable,
                    relOp,
                    pushDown,
                    isLookUp,
                    scanTs,
                    transaction.getIsolationLevel(),
                    transaction.getLockTimeOut(),
                    resultSelection
                );
                vertex = new Vertex(TXN_PART_DOCUMENT, param);
            }
            Task task = job.getOrCreate(currentLocation, idGenerator);
            OutputHint hint = new OutputHint();
            hint.setPartId(rangeDistribution.id());
            vertex.setHint(hint);
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            outputs.add(vertex);
        }
        visitor.setScan(true);
        return outputs;
    }

    public static String getDocumentQueryStr(List<Object> operandsList) {
        return Objects.requireNonNull((SqlCharStringLiteral)operandsList.get(2)).getStringValue();
    }

    public static String getDocuementTabName(List<Object> operandsList) {
        return operandsList.get(0).toString();
    }

    public static String getDocuementIndexName(List<Object> operandsList) {
        return operandsList.get(1).toString();
    }

    public static Integer getDocumentTopK(@NonNull List<Object> operandsList) {
        return ((Number) Objects.requireNonNull(((SqlNumericLiteral) operandsList.get(3)).getValue())).intValue();
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
            .filter(i -> i < table.columns.size())
            .map(i -> table.getColumns().get(i))
            .filter(col -> !col.isPrimary())
            .collect(Collectors.toList());
        if (selectionColList.isEmpty()) {
            return false;
        }
        boolean recognizableTypes = selectionColList.stream()
            .allMatch(column -> pushDownSchemaList.contains(column.type.getClass()));
        if (!recognizableTypes) {
            return false;
        }

        // document index only with field can push down
        List<Column> filterIndexCols = indexTable.getColumns().stream()
            .filter(col -> !col.isPrimary() && !(col.getType() instanceof ListType))
            .collect(Collectors.toList());
        java.util.Optional<Column> optional = selectionColList.stream()
            .filter(column -> !filterIndexCols.contains(column))
            .findFirst();
        return !optional.isPresent();
    }

}
