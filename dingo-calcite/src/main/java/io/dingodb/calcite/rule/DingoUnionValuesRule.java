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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexLiteral;

import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;

public class DingoUnionValuesRule extends RelRule<DingoUnionValuesRule.Config> implements SubstitutionRule {
    private DingoUnionValuesRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        Union union = call.rel(0);
        ImmutableList.Builder<ImmutableList<RexLiteral>> builder = ImmutableList.builder();
        List<RelNode> notValues = new LinkedList<>();
        for (RelNode node : union.getInputs()) {
            boolean added = false;
            // Always a `RelSubset`?
            if (node instanceof RelSubset) {
                for (RelNode n : ((RelSubset) node).getRels()) {
                    if (n instanceof Values) {
                        builder.addAll(((Values) n).getTuples());
                        added = true;
                        break;
                    }
                }
            } else if (node instanceof Values) {
                builder.addAll(((Values) node).getTuples());
                added = true;
            }
            if (!added) {
                notValues.add(node);
            }
        }
        ImmutableList<ImmutableList<RexLiteral>> tuples = builder.build();
        if (!tuples.isEmpty()) {
            LogicalValues values = LogicalValues.create(
                union.getCluster(),
                union.getRowType(),
                tuples
            );
            if (notValues.isEmpty()) {
                call.transformTo(values);
            } else {
                notValues.add(values);
                call.transformTo(LogicalUnion.create(
                    notValues,
                    union.all
                ));
            }
        }
    }

    @Override
    public boolean autoPruneOld() {
        return true;
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = EMPTY.withDescription("DingoUnionValuesRule")
            .withOperandSupplier(b0 ->
                b0.operand(Union.class).predicate(union -> union.all).anyInputs()
            )
            .as(Config.class);

        @Override
        default DingoUnionValuesRule toRule() {
            return new DingoUnionValuesRule(this);
        }
    }
}
