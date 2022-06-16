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

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DataUtils;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.DingoExprCompileException;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.List;
import javax.annotation.Nonnull;

@Value.Enclosing
public class DingoValuesReduceRule extends RelRule<DingoValuesReduceRule.Config> implements SubstitutionRule {
    protected DingoValuesReduceRule(Config config) {
        super(config);
    }

    private static void matchProject(@Nonnull DingoValuesReduceRule rule, @Nonnull RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalValues values = call.rel(1);
        final List<RexNode> projects = project.getProjects();
        RexBuilder rexBuilder = values.getCluster().getRexBuilder();
        // Find reducible expressions.
        final MyRexShuttle shuttle = new MyRexShuttle();
        final ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder = ImmutableList.builder();
        for (final ImmutableList<RexLiteral> literalList : values.getTuples()) {
            shuttle.literalList = literalList;
            final ImmutableList<RexLiteral> valuesList;
            final ImmutableList.Builder<RexLiteral> tupleBuilder = ImmutableList.builder();
            int k = 0;
            for (RexNode projectExpr : projects) {
                RelDataType type = project.getRowType().getFieldList().get(k).getType();
                RexNode e = projectExpr.accept(shuttle);
                RexLiteral o = reduceValue(rexBuilder, e, type);
                tupleBuilder.add(o);
                ++k;
            }
            valuesList = tupleBuilder.build();
            tuplesBuilder.add(valuesList);
        }
        RelDataType rowType = project.getRowType();
        RelNode newRel = LogicalValues.create(values.getCluster(), rowType, tuplesBuilder.build());
        call.transformTo(newRel);
    }

    private static void matchFilter(@Nonnull DingoValuesReduceRule rule, @Nonnull RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        LogicalValues values = call.rel(1);
        final RexNode condition = filter.getCondition();
        RexBuilder rexBuilder = values.getCluster().getRexBuilder();
        RelDataType boolType = values.getCluster().getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
        // Find reducible expressions.
        final MyRexShuttle shuttle = new MyRexShuttle();
        boolean changed = false;
        final ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder = ImmutableList.builder();
        for (final ImmutableList<RexLiteral> literalList : values.getTuples()) {
            shuttle.literalList = literalList;
            RexNode c = condition.accept(shuttle);
            RexLiteral o = reduceValue(rexBuilder, c, boolType);
            if (!o.isAlwaysTrue()) {
                changed = true;
                continue;
            }
            tuplesBuilder.add(literalList);
        }
        if (changed) {
            final RelDataType rowType;
            rowType = values.getRowType();
            RelNode newRel = LogicalValues.create(values.getCluster(), rowType, tuplesBuilder.build());
            call.transformTo(newRel);
        } else {
            call.transformTo(values);
        }
    }

    private static RexLiteral reduceValue(RexBuilder rexBuilder, RexNode in, RelDataType type) {
        Expr expr = RexConverter.convert(in);
        try {
            Object value = expr.compileIn(null).eval(null);
            if (value == null) {
                return rexBuilder.makeNullLiteral(type);
            } else {
                return rexBuilder.makeLiteral(DataUtils.toCalcite(value, type.getSqlTypeName()), type);
            }
        } catch (FailGetEvaluator | DingoExprCompileException e) {
            throw new RuntimeException(e);
        }
    }

    protected static void apply(
        RelOptRuleCall call,
        @Nullable LogicalProject project,
        @Nullable LogicalFilter filter,
        @Nonnull LogicalValues values
    ) {
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        config.matchHandler().accept(this, call);
    }

    @Override
    public boolean autoPruneOld() {
        return true;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config FILTER = ImmutableDingoValuesReduceRule.Config.builder()
            .description("DingoValuesReduceRule(Filter)")
            .operandSupplier(b0 ->
                b0.operand(LogicalFilter.class).oneInput(b1 ->
                    b1.operand(LogicalValues.class)
                        .predicate(Values::isNotEmpty).noInputs()
                )
            )
            .matchHandler(DingoValuesReduceRule::matchFilter)
            .build();

        Config PROJECT = ImmutableDingoValuesReduceRule.Config.builder()
            .description("DingoValuesReduceRule(Project)")
            .operandSupplier(b0 ->
                b0.operand(LogicalProject.class).oneInput(b1 ->
                    b1.operand(LogicalValues.class)
                        .predicate(Values::isNotEmpty).noInputs()
                )
            )
            .matchHandler(DingoValuesReduceRule::matchProject)
            .build();

        @Override
        default DingoValuesReduceRule toRule() {
            return new DingoValuesReduceRule(this);
        }

        @Value.Parameter
        MatchHandler<DingoValuesReduceRule> matchHandler();
    }

    private static class MyRexShuttle extends RexShuttle {
        private List<RexLiteral> literalList;

        @Override
        public RexNode visitInputRef(@Nonnull RexInputRef inputRef) {
            return literalList.get(inputRef.getIndex());
        }
    }
}
