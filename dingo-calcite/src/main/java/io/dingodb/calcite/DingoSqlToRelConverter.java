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

package io.dingodb.calcite;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.rel.DingoFunctionScan;
import io.dingodb.calcite.rel.LogicalDingoDiskAnnBuild;
import io.dingodb.calcite.rel.LogicalDingoDiskAnnCountMemory;
import io.dingodb.calcite.rel.LogicalDingoDiskAnnLoad;
import io.dingodb.calcite.rel.LogicalDingoDiskAnnReset;
import io.dingodb.calcite.rel.LogicalDingoDiskAnnStatus;
import io.dingodb.calcite.rel.LogicalDingoDocument;
import io.dingodb.calcite.rel.LogicalDingoVector;
import io.dingodb.calcite.rel.logical.LogicalTableModify;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.common.table.DiskAnnTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.TableDiskAnnFunctionNamespace;
import org.apache.calcite.sql.validate.TableFunctionNamespace;
import org.apache.calcite.sql.validate.TableHybridFunctionNamespace;
import org.apache.calcite.sql2rel.SqlDiskAnnOperator;
import org.apache.calcite.sql2rel.SqlDocumentOperator;
import org.apache.calcite.sql2rel.SqlFunctionScanOperator;
import org.apache.calcite.sql2rel.SqlHybridSearchOperator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.SqlVectorOperator;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class DingoSqlToRelConverter extends SqlToRelConverter {

    static final Config CONFIG = SqlToRelConverter.CONFIG
        .withTrimUnusedFields(true)
        .withExpand(false)
        .withInSubQueryThreshold(1000)
        // Disable simplify to use Dingo's own expr evaluation.
        .addRelBuilderConfigTransform(c -> c.withSimplify(false));

    DingoSqlToRelConverter(
        RelOptTable.ViewExpander viewExpander,
        @Nullable SqlValidator validator,
        Prepare.CatalogReader catalogReader,
        RelOptCluster cluster,
        boolean isExplain,
        HintStrategyTable hintStrategyTable
    ) {
        super(
            viewExpander,
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            CONFIG.withExplain(isExplain).withHintStrategyTable(hintStrategyTable)
        );
    }

    @Override
    protected @Nullable RexNode convertExtendedExpression(@NonNull SqlNode node, Blackboard bb) {
        // MySQL dialect
        if (node.getKind() == SqlKind.OTHER_FUNCTION) {
            SqlOperator operator = ((SqlCall) node).getOperator();
            // Override `substring` function to avoid complicated conversion in Calcite.
            if (operator.isName("substring", false)) {
                // The same of `this.rexBuilder`.
                RexBuilder rb = bb.getRexBuilder();
                List<RexNode> operands = ((SqlCall) node).getOperandList().stream()
                    .map(bb::convertExpression)
                    .collect(Collectors.toList());
                return rb.makeCall(SqlStdOperatorTable.SUBSTRING, operands);
            }
        }
        return null;
    }

    @Override
    protected void convertFrom(Blackboard bb, @Nullable SqlNode from) {
        if (from != null && from.getKind() == SqlKind.COLLECTION_TABLE) {
            convertCollectionTable(bb, (SqlCall) from);
            return;
        }
        super.convertFrom(bb, from);
    }

    @Override
    protected RelNode convertInsert(SqlInsert call) {
        RelOptTable targetTable = getTargetTable(call);

        final RelDataType targetRowType =
            Objects.requireNonNull(validator, "validator").getValidatedNodeType(call);
        assert targetRowType != null;
        RelNode sourceRel = convertQueryRecursive(call.getSource(), true, targetRowType).project();
        RelNode messageRel = convertColumnList(call, sourceRel);
        final List<String> targetColumnNames = new ArrayList<>();
        ImmutableList.Builder<RexNode> rexNodeSourceExpressionListBuilder = null;
        if (call instanceof io.dingodb.calcite.grammar.dml.SqlInsert) {
            io.dingodb.calcite.grammar.dml.SqlInsert sqlInsert = (io.dingodb.calcite.grammar.dml.SqlInsert) call;
            SqlNodeList targetColumnList = sqlInsert.getTargetColumnList2();
            if (targetColumnList != null && !targetColumnList.isEmpty()) {
                for (SqlNode sqlNode : targetColumnList) {
                    SqlIdentifier id = (SqlIdentifier) sqlNode;
                    RelDataTypeField field = SqlValidatorUtil.getTargetField(
                        targetTable.getRowType(), typeFactory, id, catalogReader, targetTable);
                    assert field != null : "column " + id.toString() + " not found";
                    targetColumnNames.add(field.getName());
                }
            } else {
                return super.convertInsert(call);
            }
            RelDataType sourceRowType = sourceRel.getRowType();
            final RexRangeRef sourceRef = rexBuilder.makeRangeReference(sourceRowType, 0, false);
            final Blackboard bb = createInsertBlackboard(targetTable, sourceRef, targetColumnNames);
            rexNodeSourceExpressionListBuilder = ImmutableList.builder();
            for (SqlNode n : sqlInsert.getSourceExpressionList()) {
                RexNode rn = bb.convertExpression(n);
                rexNodeSourceExpressionListBuilder.add(rn);
            }

        }

        return createModify(targetTable, messageRel, targetColumnNames, rexNodeSourceExpressionListBuilder.build());
    }

    private RelNode createModify(RelOptTable targetTable,
                                 RelNode source,
                                 List<String> targetColumnNames,
                                 List<RexNode> sourceExpressionList) {
        final ModifiableTable modifiableTable =
            targetTable.unwrap(ModifiableTable.class);
        if (modifiableTable != null
            && modifiableTable == targetTable.unwrap(Table.class)) {
            return modifiableTable.toModificationRel(cluster, targetTable,
                catalogReader, source, LogicalTableModify.Operation.INSERT, null,
                null, false);
        }
        return LogicalTableModify.create(
            targetTable,
            catalogReader,
            source,
            TableModify.Operation.INSERT,
            null,
            null,
            false,
            targetColumnNames,
            sourceExpressionList);
    }

    private Blackboard createInsertBlackboard(
        RelOptTable targetTable,
        RexNode sourceRef,
        List<String> targetColumnNames
    ) {
        final Map<String, RexNode> nameToNameMap = new HashMap<>();
        int j = 0;

        final List<ColumnStrategy> strategies = targetTable.getColumnStrategies();
        final List<String> targetFields = targetTable.getRowType().getFieldNames();
        for (String targetColumnName : targetColumnNames) {
            final int i = targetFields.indexOf(targetColumnName);
            switch (strategies.get(i)) {
                case STORED:
                case VIRTUAL:
                    break;
                default:
                    nameToNameMap.put(targetColumnName,
                        rexBuilder.makeFieldAccess(sourceRef, j++));
            }
        }
        return createBlackboard(null, nameToNameMap, false);
    }

    @Override
    protected void convertCollectionTable(Blackboard bb, SqlCall call) {
        final SqlOperator operator = call.getOperator();
        if (!(operator instanceof SqlFunctionScanOperator)
            && !(operator instanceof SqlVectorOperator)
            && !(operator instanceof SqlDocumentOperator)
            && !(operator instanceof SqlHybridSearchOperator)
            && !(operator instanceof SqlDiskAnnOperator)
        ) {
            super.convertCollectionTable(bb, call);
            return;
        }

        RelTraitSet traits = cluster.traitSetOf(DingoConvention.NONE);
        RexNode rexCall = bb.convertExpression(call);
        assert validator != null;
        RelNode callRel = null;
        if (validator.getNamespace(call) instanceof TableFunctionNamespace) {
            TableFunctionNamespace namespace = (TableFunctionNamespace) validator.getNamespace(call);
            if (operator instanceof SqlFunctionScanOperator) {
                assert namespace != null;
                callRel = new DingoFunctionScan(
                    cluster,
                    traits,
                    (RexCall) rexCall,
                    namespace.getTable(),
                    call.getOperandList()
                );
            } else if (operator instanceof SqlVectorOperator) {
                assert namespace != null;
                List<Object> operands = new ArrayList<>(call.getOperandList());
                callRel = new LogicalDingoVector(
                    cluster,
                    traits,
                    (RexCall) rexCall,
                    namespace.getTable(),
                    operands,
                    namespace.getIndex().getTableId(),
                    namespace.getIndex(),
                    null,
                    null,
                    new ArrayList<>()
                );
            } else if (operator instanceof SqlDocumentOperator) {
                assert namespace != null;
                List<Object> operands = new ArrayList<>(call.getOperandList());
                callRel = new LogicalDingoDocument(
                    cluster,
                    traits,
                    (RexCall) rexCall,
                    namespace.getTable(),
                    operands,
                    namespace.getIndex().getTableId(),
                    namespace.getIndex(),
                    null,
                    null,
                    new ArrayList<>(),
                    false
                );
            }
        } else if (validator.getNamespace(call) instanceof TableHybridFunctionNamespace) {
            TableHybridFunctionNamespace namespace = (TableHybridFunctionNamespace) validator.getNamespace(call);

            if (operator instanceof SqlHybridSearchOperator) {
                assert namespace != null;
                throw new RuntimeException("Not support convert hybrid search node.");
            }
        } else if (validator.getNamespace(call) instanceof TableDiskAnnFunctionNamespace) {
            TableDiskAnnFunctionNamespace namespace = (TableDiskAnnFunctionNamespace) validator.getNamespace(call);

            if (operator instanceof SqlDiskAnnOperator) {
                assert namespace != null;
                List<Object> operands = new ArrayList<>(call.getOperandList());
                String funcName = call.getOperator().getName();
                if (funcName.equals(DiskAnnTable.TABLE_BUILD_NAME)) {
                    callRel = new LogicalDingoDiskAnnBuild(
                        cluster,
                        traits,
                        (RexCall) rexCall,
                        namespace.getTable(),
                        operands,
                        namespace.getIndex().getTableId(),
                        namespace.getIndex(),
                        null,
                        null,
                        new ArrayList<>()
                    );
                } else if (funcName.equals(DiskAnnTable.TABLE_LOAD_NAME)) {
                    callRel = new LogicalDingoDiskAnnLoad(
                        cluster,
                        traits,
                        (RexCall) rexCall,
                        namespace.getTable(),
                        operands,
                        namespace.getIndex().getTableId(),
                        namespace.getIndex(),
                        null,
                        null,
                        new ArrayList<>()
                    );
                } else if (funcName.equals(DiskAnnTable.TABLE_STATUS_NAME)) {
                    callRel = new LogicalDingoDiskAnnStatus(
                        cluster,
                        traits,
                        (RexCall) rexCall,
                        namespace.getTable(),
                        operands,
                        namespace.getIndex().getTableId(),
                        namespace.getIndex(),
                        null,
                        null,
                        new ArrayList<>()
                    );
                } else if (funcName.equals(DiskAnnTable.TABLE_RESET_NAME)) {
                    callRel = new LogicalDingoDiskAnnReset(
                        cluster,
                        traits,
                        (RexCall) rexCall,
                        namespace.getTable(),
                        operands,
                        namespace.getIndex().getTableId(),
                        namespace.getIndex(),
                        null,
                        null,
                        new ArrayList<>()
                    );
                } else if (funcName.equals(DiskAnnTable.TABLE_COUNT_MEMORY_NAME)) {
                    callRel = new LogicalDingoDiskAnnCountMemory(
                        cluster,
                        traits,
                        (RexCall) rexCall,
                        namespace.getTable(),
                        operands,
                        namespace.getIndex().getTableId(),
                        namespace.getIndex(),
                        null,
                        null,
                        new ArrayList<>()
                    );
                }
            }
        }

        bb.setRoot(callRel, true);
    }
}
