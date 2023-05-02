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

import io.dingodb.calcite.rel.DingoPartCountDelete;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

import java.util.List;

@Slf4j
@Value.Enclosing
public class DingoPartCountRule extends RelRule<DingoPartCountRule.Config> {
    public DingoPartCountRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        // todo count or delete must have range
        //
        //final LogicalAggregate aggregate = call.rel(0);
        //final LogicalDingoTableScan scan = call.rel(1);
        //RelTraitSet traits = scan.getTraitSet()
        //    .replace(DingoConvention.INSTANCE)
        //    .replace(DingoRelStreaming.of(scan.getTable()));
        //call.transformTo(new DingoPartCountDelete(
        //    scan.getCluster(),
        //    traits,
        //    scan.getTable(),
        //    false,
        //    aggregate.getRowType()
        //));
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoPartCountRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(LogicalAggregate.class)
                    .predicate(x -> {
                        if (x.getGroupCount() != 0) {
                            return false;
                        }
                        List<AggregateCall> aggList = x.getAggCallList();
                        if (aggList.size() != 1) {
                            return false;
                        }
                        AggregateCall agg = aggList.get(0);
                        return agg.getAggregation().getKind() == SqlKind.COUNT && agg.getArgList().isEmpty();
                    }).oneInput(b1 ->
                        b1.operand(LogicalDingoTableScan.class)
                            .predicate(x -> x.getFilter() == null)
                            .noInputs()
                    )
            )
            .description("DingoPartCountRule")
            .build();

        @Override
        default DingoPartCountRule toRule() {
            return new DingoPartCountRule(this);
        }
    }
}
