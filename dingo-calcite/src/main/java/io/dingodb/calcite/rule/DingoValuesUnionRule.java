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
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

import java.util.LinkedList;
import java.util.List;

@Value.Enclosing
public class DingoValuesUnionRule extends RelRule<DingoValuesUnionRule.Config> implements SubstitutionRule {
    private DingoValuesUnionRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        Union union = call.rel(0);
        List<Object[]> tuples = new LinkedList<>();
        for (RelNode input : union.getInputs()) {
            if (input instanceof RelSubset) {
                List<RelNode> relList = ((RelSubset) input).getRelList();
                for (RelNode relNode : relList) {
                    if (relNode instanceof LogicalDingoValues) {
                        tuples.addAll(((LogicalDingoValues) relNode).getTuples());
                    }
                }
            }
        }
        if (tuples.size() == union.getInputs().size()) {
            LogicalDingoValues values = new LogicalDingoValues(
                union.getCluster(),
                union.getTraitSet(),
                union.getRowType(),
                tuples
            );
            call.transformTo(values);
        }
    }

    @Override
    public boolean autoPruneOld() {
        return true;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoValuesUnionRule.Config.builder()
            .description("DingoValuesUnionRule")
            .operandSupplier(b0 ->
                b0.operand(LogicalUnion.class).predicate(union -> union.all)
                    .inputs( // Two values can be combined.
                        b1 -> b1.operand(LogicalDingoValues.class).noInputs(),
                        b2 -> b2.operand(LogicalDingoValues.class).noInputs()
                    )
            )
            .build();

        @Override
        default DingoValuesUnionRule toRule() {
            return new DingoValuesUnionRule(this);
        }
    }
}
