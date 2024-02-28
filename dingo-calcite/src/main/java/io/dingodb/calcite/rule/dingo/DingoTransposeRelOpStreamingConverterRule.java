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

package io.dingodb.calcite.rule.dingo;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.rel.dingo.DingoRelOp;
import io.dingodb.calcite.rel.dingo.DingoStreamingConverter;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

@Value.Enclosing
public class DingoTransposeRelOpStreamingConverterRule extends RelRule<RelRule.Config> {
    protected DingoTransposeRelOpStreamingConverterRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        DingoRelOp op = call.rel(0);
        DingoStreamingConverter converter = call.rel(1);
        RelNode input = converter.getInput();
        call.transformTo(
            converter.copy(
                op.getTraitSet(),
                ImmutableList.of(op.copy(
                    input.getTraitSet(),
                    ImmutableList.of(input)
                ))
            )
        );
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoTransposeRelOpStreamingConverterRule.Config.builder()
            .description("DingoTransposeRelOpStreamingConverterRule")
            .operandSupplier(b0 ->
                b0.operand(DingoRelOp.class).oneInput(b1 ->
                    b1.operand(DingoStreamingConverter.class).anyInputs()
                )
            )
            .build();

        @Override
        default DingoTransposeRelOpStreamingConverterRule toRule() {
            return new DingoTransposeRelOpStreamingConverterRule(this);
        }
    }
}
