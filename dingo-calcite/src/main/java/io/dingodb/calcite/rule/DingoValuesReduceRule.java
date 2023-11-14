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
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.CalcValueUtils;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.exception.ElementNotExist;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

import java.util.LinkedList;
import java.util.List;

@Value.Enclosing
public class DingoValuesReduceRule extends RelRule<DingoValuesReduceRule.Config> implements SubstitutionRule {
    protected DingoValuesReduceRule(Config config) {
        super(config);
    }

    private static void matchProject(
        DingoValuesReduceRule rule,
        @NonNull RelOptRuleCall call
    ) {
        LogicalProject project = call.rel(0);
        LogicalDingoValues values = call.rel(1);
        DingoType tupleType = DefinitionMapper.mapToDingoType(values.getRowType());
        DingoType rowType = DefinitionMapper.mapToDingoType(project.getRowType());
        List<Object[]> tuples = new LinkedList<>();
        ExprConfig config = CalcValueUtils.getConfig(call);
        try {
            for (Object[] tuple : values.getTuples()) {
                tuples.add(CalcValueUtils.calcValues(project.getProjects(), rowType, tuple, tupleType, config));
            }
        } catch (ElementNotExist e) { // Means not constants.
            return;
        }
        call.transformTo(new LogicalDingoValues(
            project.getCluster(),
            project.getTraitSet(),
            project.getRowType(),
            tuples
        ));
    }

    private static void matchFilter(
        DingoValuesReduceRule rule,
        @NonNull RelOptRuleCall call
    ) {
        LogicalFilter filter = call.rel(0);
        LogicalDingoValues values = call.rel(1);
        DingoType tupleType = DefinitionMapper.mapToDingoType(values.getRowType());
        List<Object[]> tuples = new LinkedList<>();
        for (Object[] tuple : values.getTuples()) {
            Object v = CalcValueUtils.calcValue(
                filter.getCondition(),
                DingoTypeFactory.INSTANCE.scalar("BOOL", false),
                tuple,
                tupleType,
                CalcValueUtils.getConfig(call)
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
