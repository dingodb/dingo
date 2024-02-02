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

package io.dingodb.calcite.rel;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.fun.DingoOperatorTable;
import io.dingodb.calcite.stats.StatsCache;
import io.dingodb.calcite.utils.RelDataTypeUtils;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rule.DingoVectorIndexRule.getDefaultSelection;

public class LogicalDingoTableScan extends TableScan {
    @Getter
    protected final RexNode filter;
    @Getter
    protected TupleMapping selection;
    @Getter
    protected TupleMapping realSelection;
    @Getter
    protected final List<AggregateCall> aggCalls;
    @Getter
    protected final ImmutableBitSet groupSet;
    @Getter
    protected final ImmutableList<ImmutableBitSet> groupSets;
    @Getter
    protected final boolean pushDown;

    @Getter
    @Setter
    // temporary constant
    protected boolean forDml;

    @Getter
    @Setter
    protected boolean export;
    @Getter
    @Setter
    protected String outfile;

    public LogicalDingoTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        @Nullable RexNode filter,
        @Nullable TupleMapping selection
    ) {
        this(cluster, traitSet, hints, table, filter, selection, null, null, null, false, false);
    }

    public LogicalDingoTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        @Nullable RexNode filter,
        @Nullable TupleMapping selection,
        @Nullable List<AggregateCall> aggCalls,
        @Nullable ImmutableBitSet groupSet,
        @Nullable ImmutableList<ImmutableBitSet> groupSets,
        boolean pushDown,
        boolean forDml
    ) {
        super(cluster, traitSet, hints, table);
        this.filter = filter;
        this.aggCalls = aggCalls;
        this.groupSet = groupSet;
        this.groupSets = groupSets;
        this.pushDown = pushDown;
        DingoTable dingoTable = table.unwrap(DingoTable.class);
        assert dingoTable != null;
        this.realSelection = selection;
        this.forDml = forDml;
        // If the columns of the table contain hide and delete, the data shows that they need to be deleted
        if (selection != null) {
            if (forDml) {
                this.selection = selection;
            } else {
                List<Integer> mappingList = new ArrayList<>();
                for (int index : selection.getMappings()) {
                    if (dingoTable.getTable().getColumns().get(index).getState() == 1) {
                        mappingList.add(index);
                    }
                }
                this.selection = TupleMapping.of(mappingList);
            }
        } else {
            List<Integer> mapping = dingoTable.getTable()
                .getColumns()
                .stream()
                .filter(col -> col.getState() == 1)
                .map(col -> dingoTable.getTable().getColumns().indexOf(col))
                .collect(Collectors.toList());
            this.selection = TupleMapping.of(mapping);
        }
        // The vector distance function adapts to the corresponding function based on the table vector type
        // such as L2_ Distance, ip_ Distance, cosine_ Distance, etc
        if (filter != null) {
            dispatchDistanceCondition(filter, selection, dingoTable);
        }
    }

    public boolean isKey(ImmutableBitSet columns) {
        if (selection != null) {
            columns = ImmutableBitSet.of(columns.asList().stream()
                .map(selection::get)
                .collect(Collectors.toList()));
        }
        return getTable().isKey(columns);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rowCount;
        if (filter != null) {
            RelNode fakeInput = new LogicalDingoTableScan(
                getCluster(),
                getTraitSet(),
                getHints(),
                table,
                null,
                null
            );
            rowCount = RelMdUtil.estimateFilteredRows(fakeInput, filter, mq);
        } else {
            rowCount = StatsCache.getTableRowCount(this);
        }
        if (groupSet != null) {
            if (groupSet.cardinality() == 0) {
                rowCount = 1.0;
            } else {
                rowCount *= 1.0 - Math.pow(.8, groupSet.cardinality());
            }
        }
        return rowCount;
    }

    /**
     * {@inheritDoc}
     * <p>
     * NOTE: Currently, the cost is compared by row count only.
     */
    @Override
    public @Nullable RelOptCost computeSelfCost(@NonNull RelOptPlanner planner, @NonNull RelMetadataQuery mq) {
        double rowCount = estimateRowCount(mq);
        double cpu = rowCount + 1;
        return planner.getCostFactory().makeCost(rowCount, cpu, 0);
    }

    @Override
    public RelDataType deriveRowType() {
        RelDataType selected = getSelectedType();
        if (aggCalls != null) {
            return Aggregate.deriveRowType(
                getCluster().getTypeFactory(),
                selected,
                false,
                groupSet,
                groupSets,
                aggCalls
            );
        }
        return selected;
    }

    public RelDataType getSelectedType() {
        return RelDataTypeUtils.mapType(
            getCluster().getTypeFactory(),
            getTableType(),
            realSelection
        );
    }

    public RelDataType getTableType() {
        return table.getRowType();
    }

    @Override
    public @NonNull RelWriter explainTerms(@NonNull RelWriter pw) {
        super.explainTerms(pw);
        // crucial, this is how Calcite distinguish between different node with different props.
        pw.itemIf("filter", filter, filter != null);
        pw.itemIf("selection", selection, selection != null);
        pw.itemIf("groupSet", groupSet, groupSet != null);
        pw.itemIf("aggCalls", aggCalls, aggCalls != null);
        pw.itemIf("pushDown", pushDown, pushDown);
        return pw;
    }

    /**
     * The vector distance function adapts to the corresponding function based on the table vector type.
     * such as L2_ Distance, ip_ Distance, cosine_ Distance, etc.
     * @param rexPredicate condition
     * @param tupleMapping tupleMapping
     * @param dingoTable table
     */
    public static void dispatchDistanceCondition(
        RexNode rexPredicate,
        TupleMapping tupleMapping,
        DingoTable dingoTable
    ) {
        if (tupleMapping == null) {
            tupleMapping = getDefaultSelection(dingoTable);
        }
        if (rexPredicate.isA(SqlKind.AND) || rexPredicate.isA(SqlKind.OR)) {
            for (RexNode operand : ((RexCall) rexPredicate).getOperands()) {
                dispatchDistanceCondition(operand, tupleMapping, dingoTable);
            }
        } else if (rexPredicate.isA(SqlKind.COMPARISON)) {
            for (RexNode operand : ((RexCall) rexPredicate).getOperands()) {
                if (operand instanceof RexCall) {
                    RexCall rexCall = (RexCall) operand;
                    SqlOperator sqlOperator = rexCall.getOperator();
                    String name = sqlOperator.getName();
                    if (rexCall.getOperands().isEmpty()) {
                        continue;
                    }
                    RexNode ref = rexCall.getOperands().get(0);
                    if (name.equalsIgnoreCase("distance") && ref instanceof RexInputRef) {
                        RexInputRef rexInputRef = (RexInputRef) ref;
                        int colIndex = tupleMapping.get(rexInputRef.getIndex());

                        Column columnDefinition = dingoTable.getTable().getColumns().get(colIndex);
                        String metricType = getIndexMetricType(dingoTable, columnDefinition.getName());
                        SqlOperator sqlOperator1 = findSqlOperator(metricType);

                        try {
                            Field field = RexCall.class.getDeclaredField("op");
                            field.setAccessible(true);
                            field.set(rexCall, sqlOperator1);
                            field.setAccessible(false);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }

    public static String getIndexMetricType(DingoTable dingoTable, String colName) {
        Collection<IndexTable> indexDef = dingoTable.getTable().getIndexes();
        for (Table index : indexDef) {
            Optional<Column> columnOptional = index.getColumns()
                .stream().filter(column -> column.getName().equalsIgnoreCase(colName)).findFirst();
            if (columnOptional.isPresent()) {
                return index.getProperties().getProperty("metricType");
            }
        }
        return null;
    }

    public static SqlOperator findSqlOperator(String metricType) {
        if (metricType == null) {
            throw new RuntimeException("found not metric type");
        }
        String metricTypeFullName;
        switch (metricType) {
            case "L2" :
                metricTypeFullName = "l2Distance";
                break;
            case "INNER_PRODUCT":
                metricTypeFullName = "IPDistance";
                break;
            case "COSINE":
                metricTypeFullName = "cosineDistance";
                break;
            default:
                metricTypeFullName = null;
                break;
        }
        if (metricTypeFullName == null) {
            throw new RuntimeException("found not distance fun");
        }
        List<SqlOperator> sqlOperatorList = DingoOperatorTable.instance().getOperatorList();
        for (SqlOperator sqlOperator : sqlOperatorList) {
            if (sqlOperator.getName().equalsIgnoreCase(metricTypeFullName)) {
                return sqlOperator;
            }
        }
        throw new RuntimeException("found not distance fun");
    }

    public void setSelectionForDml(TupleMapping selection) {
        this.selection = selection;
        this.realSelection = selection;
        this.forDml = true;
    }
}
