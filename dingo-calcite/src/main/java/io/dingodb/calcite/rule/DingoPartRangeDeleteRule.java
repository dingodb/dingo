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
import io.dingodb.calcite.rel.DingoPartModify;
import io.dingodb.calcite.rel.DingoPartRangeDelete;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.utils.RexLiteralUtils;
import io.dingodb.calcite.utils.RuleUtils;
import io.dingodb.common.codec.Codec;
import io.dingodb.common.codec.DingoCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.util.ByteArrayUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.immutables.value.Value;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.dingodb.calcite.DingoTable.dingo;

@Slf4j
@Value.Enclosing
public class DingoPartRangeDeleteRule extends RelRule<DingoPartRangeDeleteRule.Config> {
    public DingoPartRangeDeleteRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        final DingoPartModify rel0 = call.rel(0);
        final DingoTableScan rel = call.rel(1);
        TableDefinition td = dingo(rel.getTable()).getTableDefinition();
        int firstPrimaryColumnIndex = td.getFirstPrimaryColumnIndex();
        Codec codec = new DingoCodec(Collections.singletonList(
            td.getColumn(firstPrimaryColumnIndex).getDingoType().toDingoSchema(0)), null, true);

        List<byte[]> byteList =
            calLeftAndRight(ByteArrayUtils.EMPTY_BYTES, ByteArrayUtils.MAX_BYTES, rel, firstPrimaryColumnIndex, codec);
        byte[] left = byteList.get(0);
        byte[] right = byteList.get(1);

        if (Arrays.equals(left, ByteArrayUtils.EMPTY_BYTES) || Arrays.equals(right, ByteArrayUtils.MAX_BYTES)) {
            return;
        }

        if (rel.getFilter().getKind() == SqlKind.AND && !ByteArrayUtils.lessThan(left, right)) {
            return;
        }

        call.transformTo(
            new DingoPartRangeDelete(
                rel0.getCluster(),
                rel.getTraitSet().replace(DingoConventions.DISTRIBUTED),
                rel.getTable(),
                rel0.getRowType(),
                left,
                right
            )
        );
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoPartRangeDeleteRule.Config.builder()
            .operandSupplier(
                b0 -> b0.operand(DingoPartModify.class)
                    // It is a delete operation
                    .predicate(x -> x.getOperation() == TableModify.Operation.DELETE)
                    .oneInput(b1 ->
                        b1.operand(DingoTableScan.class)
                            .predicate(r -> {
                                RexNode filter = r.getFilter();
                                // Contains filter conditions: > < and
                                if (filter != null) {
                                    SqlKind filterKind = filter.getKind();
                                    switch (filterKind) {
                                        case AND:
                                        case LESS_THAN:
                                        case LESS_THAN_OR_EQUAL:
                                        case GREATER_THAN:
                                        case GREATER_THAN_OR_EQUAL:
                                            return true;
                                        default:
                                            return false;
                                    }
                                }
                                return false;
                            }).noInputs())
            )
            .description("DingoPartRangeDeleteRule")
            .build();

        @Override
        default DingoPartRangeDeleteRule toRule() {
            return new DingoPartRangeDeleteRule(this);
        }
    }

    private List<byte[]> calLeftAndRight(
        byte[] left, byte[] right, DingoTableScan rel, int firstPrimaryColumnIndex, Codec codec)
    {
        List<byte[]> list = new ArrayList();
        switch (rel.getFilter().getKind()) {
            case AND: {
                RexCall filter = (RexCall) rel.getFilter();
                List<RexNode> operands = filter.operands;
                for (RexNode operand : operands) {
                    RuleUtils.ConditionInfo info = RuleUtils.checkCondition(operand);
                    if (info == null || info.index != firstPrimaryColumnIndex) {
                        continue;
                    }

                    try {
                        switch (info.kind) {
                            case LESS_THAN:
                            case LESS_THAN_OR_EQUAL:
                                right = codec.encodeKeyForRangeScan(new Object[]{RexLiteralUtils.convertFromRexLiteral(
                                    info.value,
                                    DingoTypeFactory.fromRelDataType(info.value.getType())
                                )});
                                break;
                            case GREATER_THAN:
                            case GREATER_THAN_OR_EQUAL:
                                left = codec.encodeKeyForRangeScan(new Object[]{RexLiteralUtils.convertFromRexLiteral(
                                    info.value,
                                    DingoTypeFactory.fromRelDataType(info.value.getType())
                                )});
                                break;
                            default:
                                break;
                        }
                    } catch (IOException e) {
                        log.error("Some errors occurred in encodeKeyForRangeScan: {}", e);
                        throw new RuntimeException(e);
                    }
                }
                break;
            }
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL: {
                RuleUtils.ConditionInfo info = RuleUtils.checkCondition(rel.getFilter());
                if (info == null || info.index != firstPrimaryColumnIndex) {
                    break;
                }
                try {
                    right = codec.encodeKeyForRangeScan(new Object[]{RexLiteralUtils.convertFromRexLiteral(
                        info.value,
                        DingoTypeFactory.fromRelDataType(info.value.getType())
                    )});
                    left = null;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                break;
            }
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL: {
                RuleUtils.ConditionInfo info = RuleUtils.checkCondition(rel.getFilter());
                if (info == null || info.index != firstPrimaryColumnIndex) {
                    break;
                }
                try {
                    left = codec.encodeKeyForRangeScan(new Object[]{RexLiteralUtils.convertFromRexLiteral(
                        info.value,
                        DingoTypeFactory.fromRelDataType(info.value.getType())
                    )});
                    right = null;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                break;
            }
            default:
                break;
        }
        list.add(left);
        list.add(right);
        return list;
    }
}
