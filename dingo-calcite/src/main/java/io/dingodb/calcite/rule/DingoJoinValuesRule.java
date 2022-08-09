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
import io.dingodb.common.util.ArrayUtils;
import io.dingodb.expr.runtime.TypeCode;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.immutables.value.Value;

import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;

@Value.Enclosing
public class DingoJoinValuesRule extends RelRule<DingoJoinValuesRule.Config> implements SubstitutionRule {
    protected DingoJoinValuesRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        LogicalDingoValues value0 = call.rel(1);
        LogicalDingoValues value1 = call.rel(2);
        List<Object[]> tuples = new LinkedList<>();
        if (join.getJoinType() == JoinRelType.INNER) {
            DingoType type = DingoTypeFactory.fromRelDataType(join.getRowType());
            for (Object[] v0 : value0.getTuples()) {
                for (Object[] v1 : value1.getTuples()) {
                    Object[] newTuple = ArrayUtils.concat(v0, v1);
                    Object v = RexConverter.calcValue(
                        join.getCondition(),
                        DingoTypeFactory.scalar(TypeCode.BOOL, false),
                        newTuple,
                        type
                    );
                    if (v != null && (boolean) v) {
                        tuples.add(newTuple);
                    }
                }
            }
            call.transformTo(new LogicalDingoValues(
                join.getCluster(),
                join.getTraitSet(),
                join.getRowType(),
                tuples
            ));
        }
    }

    @Override
    public boolean autoPruneOld() {
        return true;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoJoinValuesRule.Config.builder()
            .description("DingoJoinValuesRule(Filter)")
            .operandSupplier(b0 ->
                b0.operand(LogicalJoin.class).inputs(
                    b1 -> b1.operand(LogicalDingoValues.class).noInputs(),
                    b2 -> b2.operand(LogicalDingoValues.class).noInputs()
                )
            )
            .build();

        @Override
        default DingoJoinValuesRule toRule() {
            return new DingoJoinValuesRule(this);
        }
    }
}
