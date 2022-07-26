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

import io.dingodb.calcite.rel.LogicalDingoValues;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.converter.ExprConverter;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.DingoExprCompileException;
import io.dingodb.expr.runtime.TupleEvalContext;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Value.Enclosing
public class DingoValuesReduceRule extends RelRule<DingoValuesReduceRule.Config> implements SubstitutionRule {
    protected DingoValuesReduceRule(Config config) {
        super(config);
    }

    private static void matchProject(@Nonnull DingoValuesReduceRule rule, @Nonnull RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalDingoValues values = call.rel(1);
        DingoType tupleType = DingoTypeFactory.fromRelDataType(values.getRowType());
        DingoType rowType = DingoTypeFactory.fromRelDataType(project.getRowType());
        List<Object[]> tuples = new LinkedList<>();
        for (Object[] tuple : values.getTuples()) {
            tuples.add(calcValues(project.getProjects(), rowType, tuple, tupleType));
        }
        call.transformTo(new LogicalDingoValues(
            project.getCluster(),
            project.getTraitSet(),
            project.getRowType(),
            tuples
        ));
    }

    private static void matchFilter(@Nonnull DingoValuesReduceRule rule, @Nonnull RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        LogicalDingoValues values = call.rel(1);
        DingoType tupleType = DingoTypeFactory.fromRelDataType(values.getRowType());
        List<Object[]> tuples = new LinkedList<>();
        for (Object[] tuple : values.getTuples()) {
            Object v = calcValue(
                filter.getCondition(),
                DingoTypeFactory.scalar(TypeCode.BOOL, false),
                tuple,
                tupleType
            );
            if (v != null && (boolean) v) {
                tuples.add(tuple);
            }
        }
        call.transformTo(new LogicalDingoValues(
            filter.getCluster(),
            filter.getTraitSet(),
            filter.getRowType(),
            tuples
        ));
    }

    @Nullable
    private static Object calcValue(
        RexNode rexNode,
        @Nonnull DingoType targetType,
        Object[] tuple,
        DingoType tupleType
    ) {
        Expr expr = RexConverter.convert(rexNode);
        try {
            return targetType.convertFrom(
                expr.compileIn(tupleType).eval(new TupleEvalContext(tuple)),
                ExprConverter.INSTANCE
            );
        } catch (DingoExprCompileException | FailGetEvaluator e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private static Object[] calcValues(
        @Nonnull List<RexNode> rexNodeList,
        @Nonnull DingoType targetType,
        Object[] tuple,
        DingoType tupleType
    ) {
        return IntStream.range(0, rexNodeList.size())
            .mapToObj(i -> calcValue(
                rexNodeList.get(i),
                Objects.requireNonNull(targetType.getChild(i)),
                tuple,
                tupleType
            ))
            .toArray(Object[]::new);
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
                    b1.operand(LogicalDingoValues.class).noInputs()
                )
            )
            .matchHandler(DingoValuesReduceRule::matchFilter)
            .build();

        Config PROJECT = ImmutableDingoValuesReduceRule.Config.builder()
            .description("DingoValuesReduceRule(Project)")
            .operandSupplier(b0 ->
                b0.operand(LogicalProject.class).oneInput(b1 ->
                    b1.operand(LogicalDingoValues.class).noInputs()
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
}
