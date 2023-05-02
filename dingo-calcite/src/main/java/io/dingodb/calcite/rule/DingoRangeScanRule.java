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

import io.dingodb.calcite.rel.DingoPartRangeScan;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.RexLiteralUtils;
import io.dingodb.calcite.utils.RuleUtils;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
@Value.Enclosing
public class DingoRangeScanRule extends RelRule<DingoRangeScanRule.Config> {
    public DingoRangeScanRule(Config config) {
        super(config);
    }

    @SuppressWarnings("checkstyle:FallThrough")
    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        final DingoTableScan rel = call.rel(0);
        TableDefinition td = TableUtils.getTableDefinition(rel.getTable());
        int firstPrimaryColumnIndex = td.getFirstPrimaryColumnIndex();
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(TableUtils.getTableId(rel.getTable()), td);
        Object[] tuple = new Object[td.getColumnsCount()];
        if (rel.getFilter().getKind() == SqlKind.AND) {
            RexCall filter = (RexCall) rel.getFilter();
            byte[] left = ByteArrayUtils.EMPTY_BYTES;
            byte[] right = ByteArrayUtils.MAX_BYTES;
            boolean isNotBetween = false;
            boolean includeStart = true;
            boolean includeEnd = true;

            List<RexNode> operands = Collections.emptyList();
            // Not between and
            if (filter.op.kind == SqlKind.NOT) {
                operands = ((RexCall) filter.getOperands().get(0)).getOperands();
                isNotBetween = true;
            } else if (filter.op.kind == SqlKind.AND) {
                operands = filter.operands;
            }

            for (RexNode operand : operands) {
                RuleUtils.ConditionInfo info = RuleUtils.checkCondition(operand);
                if (info == null || info.index != firstPrimaryColumnIndex) {
                    continue;
                }
                try {
                    switch (info.kind) {
                        case LESS_THAN:
                            includeEnd = false;
                        case LESS_THAN_OR_EQUAL:
                            tuple[firstPrimaryColumnIndex] = RexLiteralUtils.convertFromRexLiteral(
                                info.value,
                                DefinitionMapper.mapToDingoType(info.value.getType())
                            );
                            right = codec.encodeKeyPrefix(tuple, 1);
                            break;
                        case GREATER_THAN:
                            includeStart = false;
                        case GREATER_THAN_OR_EQUAL:
                            tuple[firstPrimaryColumnIndex] = RexLiteralUtils.convertFromRexLiteral(
                                info.value,
                                DefinitionMapper.mapToDingoType(info.value.getType())
                            );
                            left = codec.encodeKeyPrefix(tuple, 1);
                            break;
                        default:
                            break;
                    }
                } catch (IOException e) {
                    log.error("Some errors occurred in encodeKeyForRangeScan: ", e);
                    throw new RuntimeException(e);
                }
            }
            if (Arrays.equals(left, ByteArrayUtils.EMPTY_BYTES) || Arrays.equals(right, ByteArrayUtils.MAX_BYTES)) {
                return;
            }
            if (ByteArrayUtils.lessThan(left, right)) {
                call.transformTo(
                    new DingoPartRangeScan(
                        rel.getCluster(),
                        rel.getTraitSet(),
                        rel.getHints(),
                        rel.getTable(),
                        rel.getFilter(),
                        rel.getSelection(),
                        left,
                        right,
                        isNotBetween,
                        includeStart,
                        includeEnd
                    )
                );
            }
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        DingoRangeScanRule.Config DEFAULT = ImmutableDingoRangeScanRule.Config.builder()
            .operandSupplier(
                b0 -> b0.operand(DingoTableScan.class)
                    .predicate(r -> {
                        RexNode filter = r.getFilter();
                        if (filter != null) {
                            if (filter.getKind() == SqlKind.AND) {
                                return true;
                            }

                            // Support not between and
                            if (filter.getKind() == SqlKind.NOT) {
                                for (RexNode operand : ((RexCall) filter).operands) {
                                    RexCall rexCall;
                                    if (operand instanceof RexCall) {
                                        rexCall = (RexCall) operand;
                                    } else {
                                        return false;
                                    }
                                    if (rexCall.getKind() != SqlKind.AND) {
                                        return false;
                                    }
                                }
                                return true;
                            }
                        }
                        return false;
                    }).noInputs()
            )
            .description("DingoPartRangeRule")
            .build();

        @Override
        default DingoRangeScanRule toRule() {
            return new DingoRangeScanRule(this);
        }
    }
}
