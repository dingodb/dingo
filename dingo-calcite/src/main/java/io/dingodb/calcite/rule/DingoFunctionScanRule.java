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

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

@Slf4j
@Value.Enclosing
public class DingoFunctionScanRule extends RelRule<DingoFunctionScanRule.Config> {
    public DingoFunctionScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        return;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        DingoFunctionScanRule.Config DEFAULT = ImmutableDingoFunctionScanRule.Config.builder()
            .operandSupplier(
                b0 -> b0.operand(TableFunctionScan.class)
                    .predicate(r -> {
                        return true;
                    }).noInputs()
            )
            .description("DingoFunctionScanRule")
            .build();

        @Override
        default DingoFunctionScanRule toRule() {
            return new DingoFunctionScanRule(this);
        }
    }
}
