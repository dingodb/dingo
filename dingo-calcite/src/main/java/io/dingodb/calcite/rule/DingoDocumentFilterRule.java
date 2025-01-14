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
import io.dingodb.calcite.rel.LogicalDingoDocument;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.immutables.value.Value;

@Value.Enclosing
public class DingoDocumentFilterRule extends RelRule<DingoDocumentFilterRule.Config> implements SubstitutionRule {

    public DingoDocumentFilterRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        final LogicalDingoDocument document = call.rel(1);
        call.transformTo(
            new LogicalDingoDocument(
                document.getCluster(),
                document.getTraitSet(),
                document.getCall(),
                document.getTable(),
                document.getOperands(),
                document.getIndexTableId(),
                document.getIndexTable(),
                document.getSelection(),
                filter.getCondition(),
                filter.getHints(),
                document.isDocumentScanFilter()
            )
        );
    }

    @Override
    public boolean autoPruneOld() {
        return true;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        DingoDocumentFilterRule.Config DEFAULT = ImmutableDingoDocumentFilterRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(LogicalFilter.class).oneInput(b1 ->
                    b1.operand(LogicalDingoDocument.class)
                        .predicate(rel -> {
                            boolean isFullSelection = rel.getRealSelection() == null;
                            if (!isFullSelection) {
                                DingoTable dingoTable = rel.getTable().unwrap(DingoTable.class);
                                assert dingoTable != null;
                                isFullSelection = rel.getRealSelection().size()
                                    == dingoTable.getTable().getColumns().size();
                            }
                            return isFullSelection && rel.getFilter() == null;
                        } )
                        .noInputs()
                )
            )
            .description("DingoDocumentFilterRule")
            .build();

        @Override
        default DingoDocumentFilterRule toRule() {
            return new DingoDocumentFilterRule(this);
        }
    }
}
