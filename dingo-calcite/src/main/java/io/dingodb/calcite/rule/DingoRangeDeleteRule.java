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
import io.dingodb.calcite.utils.RangeUtils;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

@Slf4j
@Value.Enclosing
public class DingoRangeDeleteRule extends RelRule<DingoRangeDeleteRule.Config> {
    public DingoRangeDeleteRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        final DingoTableModify rel1 = call.rel(0);
        final DingoTableScan rel = call.rel(1);
        TableDefinition td = TableUtils.getTableDefinition(rel.getTable());
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(td);

        RangeDistribution range = RangeUtils.createRangeByFilter(td, codec, rel.getFilter(), rel.getSelection());
        if (range != null) {
            call.transformTo(
                new DingoPartRangeDelete(
                    rel1.getCluster(),
                    rel.getTraitSet(),
                    rel.getTable(),
                    rel1.getRowType(),
                    range.getStartKey(),
                    range.getEndKey(),
                    rel.getFilter().getKind() == SqlKind.NOT,
                    range.isWithStart(),
                    range.isWithEnd()
                )
            );
        }

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
