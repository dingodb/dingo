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
import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@Value.Enclosing
public class DingoAggregateScanRule extends RelRule<RelRule.Config> {
    protected DingoAggregateScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        DingoAggregate aggregate = call.rel(0);
        DingoTableScan scan = call.rel(1);
        RelOptCluster cluster = aggregate.getCluster();
        RexNode filter = scan.getFilter();
        TupleMapping selection = scan.getSelection();
        if (!scan.isPushDown()) {
            return;
        }
        boolean isCountNoArgListAgg = aggregate.getAggCallList() != null && aggregate.getAggCallList().size() == 1
            && aggregate.getAggCallList().get(0).toString().equalsIgnoreCase("COUNT()")
            && selection == null;
        if (filter != null) {
            DingoType schema = DefinitionMapper.mapToDingoType(scan.getTableType());
            if (scan.getSelection() != null) {
                schema = schema.select(scan.getSelection());
            } else if (isCountNoArgListAgg) {
                // Optimization scenario similar to this SQL: select count(*) from t1 where sal > 1 and id = 1 and name ='a'
                final List<Integer> selectedColumns = new ArrayList<>();
                final RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
                    @Override
                    public @Nullable Void visitInputRef(@NonNull RexInputRef inputRef) {
                        if (!selectedColumns.contains(inputRef.getIndex())) {
                            selectedColumns.add(inputRef.getIndex());
                        }
                        return null;
                    }
                };
                filter.accept(visitor);
                // Order naturally to help decoding in push down.
                selectedColumns.sort(Comparator.naturalOrder());
                Mapping mapping = Mappings.target(selectedColumns, scan.getRowType().getFieldCount());
                // Push selection down over filter.
                filter = RexUtil.apply(mapping, filter);
                selection = TupleMapping.of(selectedColumns);
                schema = schema.select(selection);
            }
            byte[] code = SqlExprUtils.toSqlExpr(filter).getCoding(schema, null);
            // Do not push-down if the filter can not be pushed down.
            if (code == null) {
                return;
            }
        } else if (isCountNoArgListAgg) {
            // Optimization scenario similar to this SQL: select count(*) from t1
            Table table = scan.getTable().unwrap(DingoTable.class).getTable();
            int firstPrimaryColumnIndex = table.keyMapping().get(0);
            // selection use first primary key index
            selection = TupleMapping.of(Arrays.asList(firstPrimaryColumnIndex));
        }
        call.transformTo(
            new DingoTableScan(
                cluster,
                aggregate.getTraitSet(),
                scan.getHints(),
                scan.getTable(),
                filter,
                selection,
                aggregate.getAggCallList(),
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                scan.isPushDown(),
                scan.isForDml()
            )
        );
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoAggregateScanRule.Config.builder()
            .description("DingoAggregateScanRule")
            .operandSupplier(b0 ->
                b0.operand(DingoAggregate.class).oneInput(b1 ->
                    b1.operand(DingoTableScan.class).noInputs()
                )
            )
            .build();

        @Override
        default DingoAggregateScanRule toRule() {
            return new DingoAggregateScanRule(this);
        }
    }
}
