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

import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.SqlExprUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

@Value.Enclosing
public class DingoAggregateScanRule extends RelRule<RelRule.Config> {
    protected DingoAggregateScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        DingoAggregate aggregate = call.rel(0);
        DingoTableScan scan = call.rel(1);
        RelOptCluster cluster = aggregate.getCluster();
        RexNode filter = scan.getFilter();
        if (filter != null) {
            byte[] code = SqlExprUtils.toSqlExpr(filter)
                .getCoding(DefinitionMapper.mapToDingoType(scan.getTableType()), null);
            // Do not push-down if the filter can not be pushed down.
            if (code == null) {
                return;
            }
        }
        call.transformTo(
            new DingoTableScan(
                cluster,
                aggregate.getTraitSet(),
                scan.getHints(),
                scan.getTable(),
                scan.getFilter(),
                scan.getSelection(),
                aggregate.getAggCallList(),
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                scan.isPushDown()
            )
        );
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoAggregateScanRule.Config.builder()
            .description("DingoAggregateScanRule")
            .operandSupplier(b0 ->
                b0.operand(DingoAggregate.class).oneInput(b1 ->
                    b1.operand(DingoTableScan.class).noInputs()
                )
            )
            .build();

        @Override
        default DingoAggregateScanRule toRule() {
            return new DingoAggregateScanRule(this);
        }
    }
}
