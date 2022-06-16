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
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoExchange;
import io.dingodb.calcite.rel.DingoPartModify;
import io.dingodb.calcite.rel.DingoPartition;
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.common.table.TableDefinition;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

@Value.Enclosing
public class DingoPartModifyRule extends RelRule<DingoPartModifyRule.Config> {
    protected DingoPartModifyRule(Config config) {
        super(config);
    }

    private static void checkUpdateInPart(@Nonnull DingoTableModify rel) {
        DingoTable table = rel.getTable().unwrap(DingoTable.class);
        assert table != null;
        TableDefinition td = table.getTableDefinition();
        List<String> updateList = rel.getUpdateColumnList();
        if (updateList != null && updateList.stream().anyMatch(c ->
            Objects.requireNonNull(td.getColumn(c)).isPrimary())
        ) {
            throw new IllegalStateException(
                "Update columns " + updateList + " contain primary columns and are not supported."
            );
        }
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        DingoTableModify rel = call.rel(0);
        RelNode input = rel.getInput();
        RelNode convertedInput = null;
        RelOptCluster cluster = rel.getCluster();
        switch (rel.getOperation()) {
            case INSERT:
                convertedInput = new DingoExchange(
                    cluster,
                    rel.getTraitSet().replace(DingoConventions.DISTRIBUTED),
                    new DingoPartition(
                        cluster,
                        rel.getTraitSet().replace(DingoConventions.DISTRIBUTED),
                        convert(input, DingoConventions.DISTRIBUTED),
                        rel.getTable()
                    )
                );
                break;
            case UPDATE:
                // Only support update in part.
                checkUpdateInPart(rel);
                convertedInput = convert(input, DingoConventions.DISTRIBUTED);
                break;
            case DELETE:
                convertedInput = convert(input, DingoConventions.DISTRIBUTED);
                break;
            default:
                throw new IllegalStateException(
                    "Operation \"" + rel.getOperation() + "\" is not supported."
                );
        }
        call.transformTo(new DingoPartModify(
            cluster,
            rel.getTraitSet().replace(DingoConventions.DISTRIBUTED),
            convertedInput,
            rel.getTable(),
            rel.getOperation(),
            rel.getUpdateColumnList(),
            rel.getSourceExpressionList()
        ));
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoPartModifyRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(DingoTableModify.class).trait(DingoConventions.DINGO).anyInputs()
            )
            .description("DingoPartModifyRule")
            .build();

        @Override
        default DingoPartModifyRule toRule() {
            return new DingoPartModifyRule(this);
        }
    }
}
