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
import io.dingodb.calcite.rel.DingoPartDelete;
import io.dingodb.calcite.rel.DingoPartModify;
import io.dingodb.calcite.rel.DingoPartScan;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.rel.DingoTableScan;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;

import javax.annotation.Nonnull;

@Slf4j
public class DingoPartitionDeleteRule extends RelRule<DingoPartitionDeleteRule.Config> {
    public DingoPartitionDeleteRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        final DingoPartModify rel = call.rel(0);
        final DingoPartScan scan = call.rel(1);

        RelOptCluster cluster = rel.getCluster();
        call.transformTo(new DingoPartDelete(
            cluster,
            scan.getTraitSet().replace(DingoConventions.DISTRIBUTED),
            scan.getTable(),
            scan.getSelection()
        ));
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = EMPTY
            .withOperandSupplier(b0 ->
                b0.operand(DingoPartModify.class)
                  .predicate(x -> x.getOperation() == TableModify.Operation.DELETE)
                  .oneInput(b1 ->
                    b1.operand(DingoPartScan.class)
                        .predicate(x -> x.getFilter() == null)
                        .anyInputs()
                )
            )
            .withDescription("DingoPartitionDeleteRule")
            .as(Config.class);

        @Override
        default DingoPartitionDeleteRule toRule() {
            return new DingoPartitionDeleteRule(this);
        }
    }
}
