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
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

@Slf4j
@Value.Enclosing
public class DingoRangeScanRule extends RelRule<DingoRangeScanRule.Config> implements DingoRangeRule {
    public DingoRangeScanRule(Config config) {
        super(config);
    }

    @SuppressWarnings("checkstyle:FallThrough")
    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        final DingoTableScan rel = call.rel(0);
        TableDefinition td = TableUtils.getTableDefinition(rel.getTable());
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(td);
        RangeDistribution range = createRangeByFilter(td, codec, rel.getFilter());
        if (range != null) {
            call.transformTo(new DingoPartRangeScan(
                rel.getCluster(),
                rel.getTraitSet(),
                rel.getHints(),
                rel.getTable(),
                rel.getFilter(),
                rel.getSelection(),
                rel.getAggCalls(),
                rel.getGroupSet(),
                rel.getGroupSets(),
                range.getStartKey(),
                range.getEndKey(),
                rel.getFilter().getKind() == SqlKind.NOT,
                range.isWithStart(),
                range.isWithEnd(),
                rel.isPushDown()
            ));
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
