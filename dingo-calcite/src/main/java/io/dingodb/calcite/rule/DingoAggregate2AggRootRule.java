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
import io.dingodb.calcite.rel.DingoAggregate;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.sql.SqlKind;

import javax.annotation.Nonnull;

public class DingoAggregate2AggRootRule extends RelRule<DingoAggregate2AggRootRule.Config> {
    protected DingoAggregate2AggRootRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        Aggregate rel = call.rel(0);

        // AVG must be transformed to SUM/COUNT before.
        if (rel.getAggCallList().stream().anyMatch(agg -> agg.getAggregation().getKind() == SqlKind.AVG)) {
            return;
        }

        if (rel.getAggCallList().stream().anyMatch(DingoAggregateRule.isAggregateHasDistinct)) {
            return;
        }

        RelOptCluster cluster = rel.getCluster();
        call.transformTo(
            new DingoAggregate(
                cluster,
                rel.getTraitSet().replace(DingoConventions.DISTRIBUTED),
                rel.getHints(),
                convert(rel.getInput(), DingoConventions.ROOT),
                rel.getGroupSet(),
                rel.getGroupSets(),
                rel.getAggCallList())
        );
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = EMPTY
            .withOperandSupplier(b0 -> b0.operand(Aggregate.class).trait(Convention.NONE).anyInputs())
            .withDescription("DingoAggregateSingleRule")
            .as(Config.class);

        @Override
        default DingoAggregate2AggRootRule toRule() {
            return new DingoAggregate2AggRootRule(this);
        }
    }
}
