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

import io.dingodb.calcite.rel.DingoPartRangeDelete;
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.RexLiteralUtils;
import io.dingodb.calcite.utils.RuleUtils;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Value.Enclosing
public class DingoRangeDeleteRule extends RelRule<DingoRangeDeleteRule.Config> {
    public DingoRangeDeleteRule(Config config) {
        super(config);
    }

    private boolean[] getIncludeStartAndEnd(RexCall filter, TableDefinition td) {
        boolean includeStart = true;
        boolean includeEnd = true;
        if (filter.op.kind == SqlKind.AND) {
            for (RexNode operand : filter.operands) {
                RuleUtils.ConditionInfo info = RuleUtils.checkCondition(operand);
                if (info == null || info.index != td.getFirstPrimaryColumnIndex()) {
                    continue;
                }

                log.info("DingoPartRangeDeleteRule {}", info.kind);
                switch (info.kind) {
                    case LESS_THAN:
                        includeEnd = false;
                        break;
                    case GREATER_THAN:
                        includeStart = false;
                        break;
                    default:
                        break;
                }
            }
        } else if (filter.op.kind == SqlKind.LESS_THAN) {
            includeEnd = false;
        } else if (filter.op.kind == SqlKind.GREATER_THAN) {
            includeStart = false;
        }

        return new boolean[]{includeStart, includeEnd};
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        final DingoTableModify rel0 = call.rel(0);
        final DingoTableScan rel = call.rel(1);
        TableDefinition td = TableUtils.getTableDefinition(rel.getTable());
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(TableUtils.getTableId(rel.getTable()), td);

        List<byte[]> byteList = calLeftAndRight(
            ByteArrayUtils.EMPTY_BYTES, ByteArrayUtils.MAX_BYTES, rel, td.getFirstPrimaryColumnIndex(), codec
        );
        byte[] left = byteList.get(0);
        byte[] right = byteList.get(1);

        if (Arrays.equals(left, ByteArrayUtils.EMPTY_BYTES) || Arrays.equals(right, ByteArrayUtils.MAX_BYTES)) {
            return;
        }
        if (rel.getFilter().getKind() == SqlKind.AND && !ByteArrayUtils.lessThan(left, right)) {
            return;
        }

        boolean[] includeStartAndEnd = getIncludeStartAndEnd((RexCall) rel.getFilter(), td);

        call.transformTo(
            new DingoPartRangeDelete(
                rel0.getCluster(),
                rel.getTraitSet(),
                rel.getTable(),
                rel0.getRowType(),
                left,
                right,
                includeStartAndEnd[0],
                includeStartAndEnd[1]
            )
        );
    }

    private List<byte[]> calLeftAndRight(
        byte[] left, byte[] right, DingoTableScan rel, int firstPrimaryColumnIndex, KeyValueCodec codec) {
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
                                right = codec.encodeKeyPrefix(new Object[]{RexLiteralUtils.convertFromRexLiteral(
                                    info.value,
                                    DefinitionMapper.mapToDingoType(info.value.getType())
                                )}, 1);
                                break;
                            case GREATER_THAN:
                            case GREATER_THAN_OR_EQUAL:
                                left = codec.encodeKeyPrefix(new Object[]{RexLiteralUtils.convertFromRexLiteral(
                                    info.value,
                                    DefinitionMapper.mapToDingoType(info.value.getType())
                                )}, 1);
                                break;
                            default:
                                break;
                        }
                    } catch (IOException e) {
                        log.error("Some errors occurred in encodeKeyForRangeScan: ", e);
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
                    right = codec.encodeKeyPrefix(new Object[]{RexLiteralUtils.convertFromRexLiteral(
                        info.value,
                        DefinitionMapper.mapToDingoType(info.value.getType())
                    )}, 1);
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
                    left = codec.encodeKeyPrefix(new Object[]{RexLiteralUtils.convertFromRexLiteral(
                        info.value,
                        DefinitionMapper.mapToDingoType(info.value.getType())
                    )}, 1);
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

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoRangeDeleteRule.Config.builder()
            .operandSupplier(
                b0 -> b0.operand(DingoTableModify.class)
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
        default DingoRangeDeleteRule toRule() {
            return new DingoRangeDeleteRule(this);
        }
    }
}
