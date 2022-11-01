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
import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.rel.DingoReduce;
import io.dingodb.calcite.rel.DingoStreamingConverter;
import io.dingodb.calcite.traits.DingoRelStreaming;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

@Value.Enclosing
public class DingoAggregateReduceRule extends RelRule<RelRule.Config> {
    protected DingoAggregateReduceRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        DingoAggregate aggregate = call.rel(0);
        DingoStreamingConverter converter = call.rel(1);
        RelOptCluster cluster = aggregate.getCluster();
        call.transformTo(
            new DingoReduce(
                cluster,
                aggregate.getTraitSet(),
                converter.copy(
                    converter.getTraitSet(),
                    ImmutableList.of(aggregate.copy(
                        converter.getInput().getTraitSet(),
                        converter.getInputs()
                    ))
                ),
                aggregate.getGroupSet(),
                aggregate.getAggCallList(),
                aggregate.getInput().getRowType()
            )
        );
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoAggregateReduceRule.Config.builder()
            .description("DingoAggregateReduceRule")
            .operandSupplier(b0 ->
                b0.operand(DingoAggregate.class).trait(DingoRelStreaming.ROOT).oneInput(b1 ->
                    b1.operand(DingoStreamingConverter.class).anyInputs()
                )
            )
            .build();

        @Override
        default DingoAggregateReduceRule toRule() {
            return new DingoAggregateReduceRule(this);
        }
    }
}
