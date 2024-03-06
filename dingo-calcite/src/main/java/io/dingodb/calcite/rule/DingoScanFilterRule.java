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

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.visitor.RexConverter;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

@Value.Enclosing
public class DingoScanFilterRule extends RelRule<DingoScanFilterRule.Config> implements SubstitutionRule {
    protected DingoScanFilterRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        final LogicalDingoTableScan scan = call.rel(1);
        try {
            RexConverter.convert(filter.getCondition());
        } catch (RexConverter.UnsupportedRexNode e) {
            return;
        }
        call.transformTo(
            new LogicalDingoTableScan(
                scan.getCluster(),
                scan.getTraitSet(),
                scan.getHints(),
                scan.getTable(),
                filter.getCondition(),
                scan.getRealSelection(),
                scan.getAggCalls(),
                scan.getGroupSet(),
                scan.getGroupSets(),
                scan.isPushDown(),
                scan.isForDml()
            )
        );
    }

    @Override
    public boolean autoPruneOld() {
        return true;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoScanFilterRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(LogicalFilter.class).oneInput(b1 ->
                    b1.operand(LogicalDingoTableScan.class)
                        .predicate(rel -> {
                            boolean isFullSelection = rel.getRealSelection() == null;
                            if (!isFullSelection) {
                                DingoTable dingoTable = rel.getTable().unwrap(DingoTable.class);
                                isFullSelection = rel.getRealSelection().size()
                                    == dingoTable.getTable().getColumns().size();
                            }
                            return isFullSelection && rel.getFilter() == null;
                        })
                        .noInputs()
                )
            )
            .description("DingoScanFilterRule")
            .build();

        @Override
        default DingoScanFilterRule toRule() {
            return new DingoScanFilterRule(this);
        }
    }
}
