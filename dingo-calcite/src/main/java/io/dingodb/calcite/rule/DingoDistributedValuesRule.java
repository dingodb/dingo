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

import io.dingodb.calcite.DingoConventions;
import io.dingodb.calcite.rel.DingoDistributedValues;
import io.dingodb.calcite.rel.DingoPartModify;
import io.dingodb.calcite.rel.DingoTableModify;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Values;
import org.immutables.value.Value;

import javax.annotation.Nonnull;

@Value.Enclosing
public class DingoDistributedValuesRule extends RelRule<DingoDistributedValuesRule.Config> {
    protected DingoDistributedValuesRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        DingoTableModify modify = call.rel(0);
        Values values = call.rel(1);
        RelOptCluster cluster = modify.getCluster();
        RelOptTable table = modify.getTable();
        call.transformTo(
            new DingoPartModify(
                cluster,
                modify.getTraitSet().replace(DingoConventions.DISTRIBUTED),
                new DingoDistributedValues(
                    cluster,
                    values.getRowType(),
                    values.getTuples(),
                    values.getTraitSet().replace(DingoConventions.DISTRIBUTED),
                    table
                ),
                table,
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList()
            )
        );
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoDistributedValuesRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(DingoTableModify.class).trait(DingoConventions.DINGO).oneInput(b1 ->
                    b1.operand(Values.class).noInputs()
                )
            )
            .description("DingoDistributedValuesRule")
            .build();

        @Override
        default DingoDistributedValuesRule toRule() {
            return new DingoDistributedValuesRule(this);
        }
    }
}
