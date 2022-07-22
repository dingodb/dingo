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
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.util.ByteArrayUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.immutables.value.Value;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static io.dingodb.calcite.DingoTable.dingo;

@Slf4j
@Value.Enclosing
public class DingoPartRangeRule extends RelRule<DingoPartRangeRule.Config> {
    public DingoPartRangeRule(Config config) {
        super(config);
    }

    @Nullable
    private static ConditionInfo getConditionInfo(@Nonnull RexCall rexCall, SqlKind reverseKind) {
        RexNode op0 = rexCall.operands.get(0);
        RexNode op1 = rexCall.operands.get(1);
        ConditionInfo info = new ConditionInfo();
        if (checkConditionOp(op0, op1, info)) {
            info.kind = rexCall.getKind();
        } else if (checkConditionOp(op1, op0, info)) {
            info.kind = reverseKind;
        } else {
            return null;
        }
        return info;
    }

    private static boolean checkConditionOp(@Nonnull RexNode op0, RexNode op1, ConditionInfo info) {
        if (op0.getKind() == SqlKind.INPUT_REF && op1.getKind() == SqlKind.LITERAL) {
            info.index = ((RexInputRef) op0).getIndex();
            info.value = (RexLiteral) op1;
            return true;
        }
        return false;
    }

    @Nullable
    private static ConditionInfo checkCondition(@Nonnull RexNode rexNode) {
        switch (rexNode.getKind()) {
            case LESS_THAN:
                return getConditionInfo((RexCall) rexNode, SqlKind.GREATER_THAN);
            case LESS_THAN_OR_EQUAL:
                return getConditionInfo((RexCall) rexNode, SqlKind.GREATER_THAN_OR_EQUAL);
            case GREATER_THAN:
                return getConditionInfo((RexCall) rexNode, SqlKind.LESS_THAN);
            case GREATER_THAN_OR_EQUAL:
                return getConditionInfo((RexCall) rexNode, SqlKind.LESS_THAN_OR_EQUAL);
            default:
                break;
        }
        return null;
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        final DingoTableScan rel = call.rel(0);
        TableDefinition td = dingo(rel.getTable()).getTableDefinition();
        int firstPrimaryColumnIndex = td.getFirstPrimaryColumnIndex();
        Codec codec = new DingoCodec(Collections.singletonList(
            td.getColumn(firstPrimaryColumnIndex).getElementType().toDingoSchema(0)
        ));
        if (rel.getFilter().getKind() == SqlKind.AND) {
            RexCall filter = (RexCall) rel.getFilter();
            byte[] left = ByteArrayUtils.EMPTY_BYTES;
            byte[] right = ByteArrayUtils.MAX_BYTES;
            boolean includeStart = true;
            boolean includeEnd = true;
            for (RexNode operand : filter.operands) {
                ConditionInfo info = checkCondition(operand);
                if (info == null || info.index != firstPrimaryColumnIndex) {
                    continue;
                }
                try {
                    switch (info.kind) {
                        case LESS_THAN:
                            includeEnd = false;
                        case LESS_THAN_OR_EQUAL:
                            right = codec.encode(new Object[]{RexConverter.convertFromRexLiteral(
                                info.value,
                                DingoTypeFactory.fromRelDataType(rel.getRowType().getFieldList().get(info.index).getType())
                            )});
                            break;
                        case GREATER_THAN:
                            includeStart = false;
                        case GREATER_THAN_OR_EQUAL:
                            left = codec.encode(new Object[]{RexConverter.convertFromRexLiteral(
                                info.value,
                                DingoTypeFactory.fromRelDataType(rel.getRowType().getFieldList().get(info.index).getType())
                            )});
                            break;
                        default:
                            break;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (Arrays.equals(left, ByteArrayUtils.EMPTY_BYTES) && Arrays.equals(right, ByteArrayUtils.MAX_BYTES)) {
                return;
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
                    RexNode filter = r.getFilter();
                    return filter != null && filter.getKind() == SqlKind.AND;
                }).noInputs()
            )
            .description("DingoPartRangeRule")
            .build();

        @Override
        default DingoPartRangeRule toRule() {
            return new DingoPartRangeRule(this);
        }
    }

    static class ConditionInfo {
        private SqlKind kind;
        private int index;
        private RexLiteral value;
    }
}
