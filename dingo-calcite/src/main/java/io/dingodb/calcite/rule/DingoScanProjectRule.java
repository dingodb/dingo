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

package io.dingodb.calcite.rule;

import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.grammar.SqlUserDefinedOperators;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.rel.LogicalDingoVector;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.dingodb.calcite.type.converter.DefinitionMapper.mapToRelDataType;
import static org.apache.calcite.sql.validate.TableFunctionNamespace.getVectorIndexTable;

@Slf4j
@Value.Enclosing
public class DingoScanProjectRule extends RelRule<DingoScanProjectRule.Config> implements SubstitutionRule {
    protected DingoScanProjectRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final LogicalDingoTableScan scan = call.rel(1);
        final List<Integer> selectedColumns = new ArrayList<>();
        final List<RexCall> vectorSelected = new ArrayList<>();
        final RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
            @Override
            public @Nullable Void visitInputRef(@NonNull RexInputRef inputRef) {
                if (!selectedColumns.contains(inputRef.getIndex())) {
                    selectedColumns.add(inputRef.getIndex());
                }
                return null;
            }

            @Override
            public Void visitCall(RexCall call) {
                if (call.op.getName().equalsIgnoreCase(SqlUserDefinedOperators.COSINE_SIMILARITY.getName())
                    || call.op.getName().equalsIgnoreCase(SqlUserDefinedOperators.IP_DISTANCE.getName())
                    || call.op.getName().equalsIgnoreCase(SqlUserDefinedOperators.L2_DISTANCE.getName())) {
                    vectorSelected.add(call);
                }
                return super.visitCall(call);
            }
        };
        List<RexNode> projects = project.getProjects();
        RexNode filter = scan.getFilter();
        visitor.visitEach(projects);
        if (filter != null) {
            filter.accept(visitor);
        }
        // Order naturally to help decoding in push down.
        selectedColumns.sort(Comparator.naturalOrder());
        Mapping mapping = Mappings.target(selectedColumns, scan.getRowType().getFieldCount());
        // Push selection down over filter.
        RexNode newFilter = (filter != null) ? RexUtil.apply(mapping, filter) : null;
        LogicalDingoTableScan newScan = new LogicalDingoTableScan(
            scan.getCluster(),
            scan.getTraitSet(),
            scan.getHints(),
            scan.getTable(),
            newFilter,
            TupleMapping.of(selectedColumns),
            scan.getAggCalls(),
            scan.getGroupSet(),
            scan.getGroupSets(),
            scan.isPushDown(),
            scan.isForDml()
        );
        final List<RexNode> newProjectRexNodes = RexUtil.apply(mapping, project.getProjects());
        if (RexUtil.isIdentity(newProjectRexNodes, newScan.getRowType())) {
            call.transformTo(newScan);
        } else {
            // for example:
            // select feature <-> array[1,2] as store from table [ order by store limit 10 ]
            // select l2Distance(feature, array[1,2]) as store from table [ order by store limit 10]
            // feature is vector index
            // transform to post filtering
//            if (vectorSelected.size() > 0) {
//                LogicalProject logicalProject = getPostVectorFiltering(
//                    project,
//                    scan,
//                    vectorSelected,
//                    newProjectRexNodes);
//                if (logicalProject != null) {
//                    call.transformTo(logicalProject);
//                }
//                return;
//            }
            call.transformTo(
                new LogicalProject(
                    project.getCluster(),
                    project.getTraitSet(),
                    project.getHints(),
                    newScan,
                    newProjectRexNodes,
                    project.getRowType(),
                    project.getVariablesSet()
                )
            );
        }
    }

    private static LogicalProject getPostVectorFiltering(LogicalProject project,
                                            LogicalDingoTableScan scan,
                                            List<RexCall> vectorSelected,
                                            List<RexNode> newProjectRexNodes) {
        LogicalDingoVector vector = getVectorScan(scan, vectorSelected.get(0));
        if (vector == null) {
            return null;
        }
        RexInputRef inputRef = new RexInputRef(vector.getRowType().getFieldList().size() - 1,
            scan.getCluster().getTypeFactory().createSqlType(SqlTypeName.FLOAT));
        AtomicBoolean rn = new AtomicBoolean(false);
        List<RexNode> newMappingPro = newProjectRexNodes
            .stream()
            .map(rexNode -> {
                if (rexNode instanceof RexCall && !rn.get()) {
                    RexCall rexCall = (RexCall) rexNode;
                    String opName = rexCall.op.getName();
                    if (opName.equalsIgnoreCase(SqlUserDefinedOperators.COSINE_SIMILARITY.getName())
                        || opName.equalsIgnoreCase(SqlUserDefinedOperators.IP_DISTANCE.getName())
                        || opName.equalsIgnoreCase(SqlUserDefinedOperators.L2_DISTANCE.getName())) {
                        rn.set(true);
                        return inputRef;
                    }
                }
                return rexNode;
            })
            .collect(Collectors.toList());
        return new LogicalProject(
            project.getCluster(),
            project.getTraitSet(),
            project.getHints(),
            vector,
            newMappingPro,
            project.getRowType(),
            project.getVariablesSet()
        );
    }

    public static LogicalDingoVector getVectorScan(LogicalDingoTableScan scan,
                                            RexCall vectorCall) {
        DingoTable dingoTable = scan.getTable().unwrap(DingoTable.class);
        assert dingoTable != null;
        List<Integer> inputRefList = vectorCall.operands.stream()
            .filter(operand -> operand instanceof RexInputRef)
            .map(rexNode -> (RexInputRef) rexNode)
            .map(RexSlot::getIndex)
            .collect(Collectors.toList());
        String col = dingoTable.getTable().getColumns().get(inputRefList.get(0)).getName().toUpperCase();

        Table useIndex;
        try {
            useIndex = getVectorIndexTable(dingoTable.getTable(), col);
        } catch (Exception e) {
            log.info(col + " vector index not found");
            return null;
        }

        List<Column> tableCols = dingoTable.getTable().getColumns();
        ArrayList<Column> cols = new ArrayList<>(tableCols.size() + 1);
        cols.addAll(tableCols);
        cols.add(Column
            .builder()
            .name("STORE")
            .sqlTypeName("FLOAT")
            .type(new FloatType(false))
            .precision(-1)
            .scale(-2147483648)
            .build()
        );
        RelDataTypeFactory typeFactory = scan.getCluster().getTypeFactory();
        RelDataType rowType = typeFactory.createStructType(
            cols.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
            cols.stream().map(Column::getName).map(String::toUpperCase).collect(Collectors.toList())
        );

        List<Object> operands = new ArrayList<>();
        operands.add(dingoTable.getTable().getName());
        operands.add(col);
        operands.add(vectorCall.operands.get(1));
        operands.add(SqlLiteral.createExactNumeric("100", new SqlParserPos(1, 1)));
        RexCall rexCall = (RexCall) scan.getCluster()
            .getRexBuilder()
            .makeCall(rowType, vectorCall.op, Collections.EMPTY_LIST);
        return new LogicalDingoVector(
            scan.getCluster(),
            scan.getTraitSet(),
            rexCall,
            new DingoRelOptTable(Objects.requireNonNull(scan.getTable().unwrap(DingoTable.class)), null, null),
            operands,
            useIndex.getTableId(),
            useIndex,
            scan.getSelection(),
            scan.getFilter(),
            scan.getHints()
            );
    }

    /**
     * {@inheritDoc}
     * <p>
     * Left the old node to allow {@link org.apache.calcite.rel.rules.CoreRules#PROJECT_REMOVE} to work.
     */
    @Override
    public boolean autoPruneOld() {
        return false;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoScanProjectRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(LogicalProject.class).oneInput(b1 ->
                    b1.operand(LogicalDingoTableScan.class)
                        .predicate(rel -> {
                            if (rel.getRealSelection() == null) {
                                return true;
                            }
                            DingoTable dingoTable = rel.getTable().unwrap(DingoTable.class);
                            assert dingoTable != null;
                            return rel.getRealSelection().size() == dingoTable.getTable().getColumns().size();
                        } )
                        .noInputs()
                )
            )
            .description("DingoScanProjectRule")
            .build();

        @Override
        default DingoScanProjectRule toRule() {
            return new DingoScanProjectRule(this);
        }
    }
}
