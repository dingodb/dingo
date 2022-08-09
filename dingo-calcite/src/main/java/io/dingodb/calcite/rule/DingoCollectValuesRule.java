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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;

import java.util.Collections;
import javax.annotation.Nonnull;

@Value.Enclosing
public class DingoCollectValuesRule extends RelRule<DingoCollectValuesRule.Config> implements SubstitutionRule {
    protected DingoCollectValuesRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        Collect collect = call.rel(0);
        LogicalDingoValues values = call.rel(1);
        if (collect.getCollectionType() == SqlTypeName.MULTISET) {
            call.transformTo(new LogicalDingoValues(
                collect.getCluster(),
                collect.getTraitSet(),
                collect.getRowType(),
                Collections.singletonList(new Object[]{values.getTuples()})
            ));
        }
    }

    @Override
    public boolean autoPruneOld() {
        return true;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoCollectValuesRule.Config.builder()
            .description("DingoCollectValuesRule(Filter)")
            .operandSupplier(b0 ->
                b0.operand(Collect.class).oneInput(b1 ->
                    b1.operand(LogicalDingoValues.class).noInputs()
                )
            )
            .build();

        @Override
        default DingoCollectValuesRule toRule() {
            return new DingoCollectValuesRule(this);
        }
    }
}
