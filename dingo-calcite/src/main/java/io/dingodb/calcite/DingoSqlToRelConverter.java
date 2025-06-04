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
import com.google.common.collect.ImmutableSet;
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
import io.dingodb.calcite.utils.DingoRelOptUtil;
import io.dingodb.common.table.DiskAnnTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.stream.Delta;
import org.apache.calcite.rel.stream.LogicalDelta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlUtil;
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
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

class DingoSqlToRelConverter extends SqlToRelConverter {

    static final Config CONFIG = SqlToRelConverter.CONFIG
        .withTrimUnusedFields(true)
        .withExpand(false)
        .withInSubQueryThreshold(1000)
        // Disable simplify to use Dingo's own expr evaluation.
        .addRelBuilderConfigTransform(c -> c.withSimplify(false));

    private final HintStrategyTable hintStrategies;

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
        this.hintStrategies = this.config.getHintStrategyTable();
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
        final List<RexNode> rexNodeSourceExpressionList = new ArrayList<>();
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
            final Blackboard bb = createInsertBlackboard(targetTable, sourceRef, targetTable.getRowType().getFieldNames());
            for (SqlNode n : sqlInsert.getSourceExpressionList()) {
                if (n.getKind() == SqlKind.IDENTIFIER) {
                    rexNodeSourceExpressionList.add(null);
                    continue;
                }
                if (n.getKind() == SqlKind.LITERAL && ((SqlLiteral) n).toValue() == null) {
                    n = SqlLiteral.createCharString(n.toString(), n.getParserPosition());
                }
                RexNode rn = bb.convertExpression(n);
                rexNodeSourceExpressionList.add(rn);
            }

        }

        return createModify(targetTable, messageRel, targetColumnNames, rexNodeSourceExpressionList);
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
        return createBlackboard(validator().getEmptyScope(), nameToNameMap, false);
    }

    private SqlValidator validator() {
        return requireNonNull(validator, "validator");
    }

    public RelRoot convertQuery(
        SqlNode query,
        final boolean needsValidation,
        final boolean top) {
        if (needsValidation) {
            query = validator().validate(query);
        }

        RelNode result = convertQueryRecursive(query, top, null).rel;
        if (top) {
            if (isStream(query)) {
                result = new LogicalDelta(cluster, result.getTraitSet(), result);
            }
        }
        RelCollation collation = RelCollations.EMPTY;
        if (!query.isA(SqlKind.DML)) {
            if (isOrdered(query)) {
                collation = requiredCollation(result);
            }
        }
        checkConvertedType(query, result);

        if (SQL2REL_LOGGER.isDebugEnabled()) {
            SQL2REL_LOGGER.debug(
                RelOptUtil.dumpPlan("Plan after converting SqlNode to RelNode",
                    result, SqlExplainFormat.TEXT,
                    SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }

        final RelDataType validatedRowType = validator().getValidatedNodeType(query);
        List<RelHint> hints = new ArrayList<>();
        if (query.getKind() == SqlKind.SELECT) {
            final SqlSelect select = (SqlSelect) query;
            if (select.hasHints()) {
                hints = SqlUtil.getRelHint(hintStrategies, select.getHints());
            }
        }

        if (config.isAddJsonTypeOperatorEnabled()) {
            result = result.accept(new NestedJsonFunctionRelRewriter());
        }

        // propagate the hints.
        result = RelOptUtil.propagateRelHints(result, false);
        return RelRoot.of(result, validatedRowType, query.getKind())
            .withCollation(collation)
            .withHints(hints);
    }

    private static boolean isStream(SqlNode query) {
        return query instanceof SqlSelect
            && ((SqlSelect) query).isKeywordPresent(SqlSelectKeyword.STREAM);
    }

    private static RelCollation requiredCollation(RelNode r) {
        if (r instanceof Sort) {
            return ((Sort) r).collation;
        }
        if (r instanceof Project) {
            return requiredCollation(((Project) r).getInput());
        }
        if (r instanceof Delta) {
            return requiredCollation(((Delta) r).getInput());
        }
        throw new AssertionError();
    }

    private void checkConvertedType(SqlNode query, RelNode result) {
        if (query.isA(SqlKind.DML)) {
            return;
        }
        // Verify that conversion from SQL to relational algebra did
        // not perturb any type information.  (We can't do this if the
        // SQL statement is something like an INSERT which has no
        // validator type information associated with its result,
        // hence the namespace check above.)
        final List<RelDataTypeField> validatedFields =
            validator().getValidatedNodeType(query).getFieldList();
        final RelDataType validatedRowType =
            validator().getTypeFactory().createStructType(
                Pair.right(validatedFields),
                SqlValidatorUtil.uniquify(Pair.left(validatedFields),
                    catalogReader.nameMatcher().isCaseSensitive()));

        final List<RelDataTypeField> convertedFields =
            result.getRowType().getFieldList().subList(0, validatedFields.size());
        final RelDataType convertedRowType =
            validator().getTypeFactory().createStructType(convertedFields);

        if (!DingoRelOptUtil.equal("validated row type", validatedRowType,
            "converted row type", convertedRowType, Litmus.IGNORE)) {
            throw new AssertionError("Conversion to relational algebra failed to "
                + "preserve datatypes:\n"
                + "validated type:\n"
                + validatedRowType.getFullTypeString()
                + "\nconverted type:\n"
                + convertedRowType.getFullTypeString()
                + "\nrel:\n"
                + RelOptUtil.toString(result));
        }
    }

    private class JsonFunctionRexRewriter extends RexShuttle {

        private final Set<Integer> jsonInputFields;

        JsonFunctionRexRewriter(Set<Integer> jsonInputFields) {
            this.jsonInputFields = jsonInputFields;
        }

        @Override public RexNode visitCall(RexCall call) {
            if (call.getOperator() == SqlStdOperatorTable.JSON_OBJECT) {
                final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
                for (int i = 0; i < call.operands.size(); ++i) {
                    if ((i & 1) == 0 && i != 0) {
                        builder.add(forceChildJsonType(call.operands.get(i)));
                    } else {
                        builder.add(call.operands.get(i));
                    }
                }
                return rexBuilder.makeCall(SqlStdOperatorTable.JSON_OBJECT, builder.build());
            }
            if (call.getOperator() == SqlStdOperatorTable.JSON_ARRAY) {
                final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
                builder.add(call.operands.get(0));
                for (int i = 1; i < call.operands.size(); ++i) {
                    builder.add(forceChildJsonType(call.operands.get(i)));
                }
                return rexBuilder.makeCall(SqlStdOperatorTable.JSON_ARRAY, builder.build());
            }
            return super.visitCall(call);
        }

        private RexNode forceChildJsonType(RexNode rexNode) {
            final RexNode childResult = rexNode.accept(this);
            if (isJsonResult(rexNode)) {
                return rexBuilder.makeCall(SqlStdOperatorTable.JSON_TYPE_OPERATOR, childResult);
            }
            return childResult;
        }

        private boolean isJsonResult(RexNode rexNode) {
            if (rexNode instanceof RexCall) {
                final RexCall call = (RexCall) rexNode;
                final SqlOperator operator = call.getOperator();
                return operator == SqlStdOperatorTable.JSON_OBJECT
                    || operator == SqlStdOperatorTable.JSON_ARRAY
                    || operator == SqlStdOperatorTable.JSON_VALUE;
            } else if (rexNode instanceof RexInputRef) {
                final RexInputRef inputRef = (RexInputRef) rexNode;
                return jsonInputFields.contains(inputRef.getIndex());
            }
            return false;
        }
    }

    private class NestedJsonFunctionRelRewriter extends RelShuttleImpl {

        @Override public RelNode visit(LogicalProject project) {
            final Set<Integer> jsonInputFields = findJsonInputs(project.getInput());
            final Set<Integer> requiredJsonFieldsFromParent = stack.size() > 0
                ? requiredJsonOutputFromParent(stack.getLast()) : Collections.emptySet();

            final List<RexNode> originalProjections = project.getProjects();
            final ImmutableList.Builder<RexNode> newProjections = ImmutableList.builder();
            JsonFunctionRexRewriter rexRewriter = new JsonFunctionRexRewriter(jsonInputFields);
            for (int i = 0; i < originalProjections.size(); ++i) {
                if (requiredJsonFieldsFromParent.contains(i)) {
                    newProjections.add(rexRewriter.forceChildJsonType(originalProjections.get(i)));
                } else {
                    newProjections.add(originalProjections.get(i).accept(rexRewriter));
                }
            }

            RelNode newInput = project.getInput().accept(this);
            return LogicalProject.create(
                newInput,
                project.getHints(),
                newProjections.build(),
                project.getRowType().getFieldNames(),
                project.getVariablesSet());
        }

        private Set<Integer> requiredJsonOutputFromParent(RelNode relNode) {
            if (!(relNode instanceof Aggregate)) {
                return Collections.emptySet();
            }
            final Aggregate aggregate = (Aggregate) relNode;
            final List<AggregateCall> aggregateCalls = aggregate.getAggCallList();
            final ImmutableSet.Builder<Integer> result = ImmutableSet.builder();
            for (final AggregateCall call : aggregateCalls) {
                if (call.getAggregation() == SqlStdOperatorTable.JSON_OBJECTAGG) {
                    result.add(call.getArgList().get(1));
                } else if (call.getAggregation() == SqlStdOperatorTable.JSON_ARRAYAGG) {
                    result.add(call.getArgList().get(0));
                }
            }
            return result.build();
        }

        private Set<Integer> findJsonInputs(RelNode relNode) {
            if (!(relNode instanceof Aggregate)) {
                return Collections.emptySet();
            }
            final Aggregate aggregate = (Aggregate) relNode;
            final List<AggregateCall> aggregateCalls = aggregate.getAggCallList();
            final ImmutableSet.Builder<Integer> result = ImmutableSet.builder();
            for (int i = 0; i < aggregateCalls.size(); ++i) {
                final AggregateCall call = aggregateCalls.get(i);
                if (call.getAggregation() == SqlStdOperatorTable.JSON_OBJECTAGG
                    || call.getAggregation() == SqlStdOperatorTable.JSON_ARRAYAGG) {
                    result.add(aggregate.getGroupCount() + i);
                }
            }
            return result.build();
        }
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
                    null,
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
