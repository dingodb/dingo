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
import io.dingodb.calcite.rel.DingoPartRangeScan;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.common.codec.Codec;
import io.dingodb.common.codec.DingoCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.immutables.value.Value;

import java.io.IOException;

import static io.dingodb.calcite.DingoTable.dingo;

@Slf4j
@Value.Enclosing
public class DingoPartRangeRule extends RelRule<DingoPartRangeRule.Config> {
    public DingoPartRangeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final DingoTableScan rel = call.rel(0);
        RexNode rexNode = RexUtil.toDnf(rel.getCluster().getRexBuilder(), rel.getFilter());
        TableDefinition td = dingo(rel.getTable()).getTableDefinition();
        KeyTuplesRexVisitor visitor = new KeyTuplesRexVisitor(td, rel.getCluster().getRexBuilder());
        Codec codec = new DingoCodec(td.getDingoSchemaOfKey());
        rexNode.accept(visitor);

        if (!visitor.isOperandHasNotPrimaryKey()) {
            RexCall filter = (RexCall) rel.getFilter();
            byte[] left = null;
            byte[] right = null;
            boolean includeStart = true;
            boolean includeEnd = true;
            for (RexNode operand : filter.operands) {
                RexCall rexCall;
                if (operand instanceof RexCall) {
                    rexCall = (RexCall) operand;
                } else {
                    return;
                }

                SqlKind opKind = rexCall.op.kind;
                if (opKind == SqlKind.LESS_THAN_OR_EQUAL || opKind == SqlKind.LESS_THAN) {
                    RexLiteral literal = (RexLiteral) rexCall.operands.get(1);
                    try {
                        right = codec.encode(new Object[]{RexConverter.convertFromRexLiteral(literal)});
                        if (opKind == SqlKind.LESS_THAN) {
                            includeEnd = false;
                        } else {
                            includeEnd = true;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                if (opKind == SqlKind.GREATER_THAN_OR_EQUAL || opKind == SqlKind.GREATER_THAN) {
                    RexLiteral literal = (RexLiteral) rexCall.operands.get(1);
                    try {
                        left = codec.encode(new Object[]{RexConverter.convertFromRexLiteral(literal)});
                        if (opKind == SqlKind.GREATER_THAN) {
                            includeStart = false;
                        } else {
                            includeStart = true;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (ByteArrayUtils.lessThan(left, right)) {
                call.transformTo(
                    new DingoPartRangeScan(
                        rel.getCluster(),
                        rel.getTraitSet().replace(DingoConventions.DISTRIBUTED),
                        rel.getHints(),
                        rel.getTable(),
                        rel.getFilter(),
                        rel.getSelection(),
                        left,
                        right,
                        includeStart,
                        includeEnd
                    )
                );
            }
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        DingoPartRangeRule.Config DEFAULT = ImmutableDingoPartRangeRule.Config.builder()
            .operandSupplier(
                b0 -> b0.operand(DingoTableScan.class).predicate(r -> {
                    RexCall filter = (RexCall) r.getFilter();
                    if (filter != null && filter.op.kind == SqlKind.AND) {
                        return true;
                    }
                    return false;
                }).noInputs()
            )
            .description("DingoPartRangeRule")
            .build();

        @Override
        default DingoPartRangeRule toRule() {
            return new DingoPartRangeRule(this);
        }
    }

}
