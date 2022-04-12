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
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.common.table.TableDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;

import static io.dingodb.calcite.DingoTable.dingo;

@Slf4j
public class DingoGetByKeysRule extends RelRule<DingoGetByKeysRule.Config> {
    public DingoGetByKeysRule(Config config) {
        super(config);
    }

    /**
     * Check the keys set to see if a full scan needed.
     *
     * @param keyTuples key tuples to be checked
     * @return <code>true</code> means the primary columns are all set for each row
     *     <code>false</code> means some columns are not set for any row, so full scan is needed
     */
    private static boolean checkKeyTuples(Set<Object[]> keyTuples) {
        if (keyTuples == null) {
            return false;
        }
        for (Object[] t : keyTuples) {
            if (Arrays.stream(t).anyMatch(Objects::isNull)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        final DingoTableScan rel = call.rel(0);
        RexNode rexNode = RexUtil.toDnf(rel.getCluster().getRexBuilder(), rel.getFilter());
        TableDefinition td = dingo(rel.getTable()).getTableDefinition();
        KeyTuplesRexVisitor visitor = new KeyTuplesRexVisitor(td);
        Set<Object[]> keyTuples = rexNode.accept(visitor);
        if (!visitor.isOperandHasNotPrimaryKey() && checkKeyTuples(keyTuples)) {
            call.transformTo(new DingoGetByKeys(
                    rel.getCluster(),
                    rel.getTraitSet().replace(DingoConventions.DISTRIBUTED),
                    rel.getTable(),
                    keyTuples,
                    rel.getSelection()
            ));
        }
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = EMPTY
            .withOperandSupplier(
                b0 -> b0.operand(DingoTableScan.class).predicate(r -> r.getFilter() != null).noInputs()
            )
            .withDescription("DingoGetByKeysRule")
            .as(Config.class);

        @Override
        default DingoGetByKeysRule toRule() {
            return new DingoGetByKeysRule(this);
        }
    }
}
