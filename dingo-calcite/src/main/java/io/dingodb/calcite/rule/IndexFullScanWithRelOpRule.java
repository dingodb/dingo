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
import io.dingodb.calcite.rel.dingo.DingoIndexScanWithRelOp;
import io.dingodb.calcite.rel.dingo.DingoRelOp;
import io.dingodb.calcite.rel.dingo.IndexFullScan;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.rel.op.UngroupedAggregateOp;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Value.Enclosing
public class IndexFullScanWithRelOpRule extends RelRule<RelRule.Config> {
    protected IndexFullScanWithRelOpRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // nonLeftMatch -> indexFullScan
        // index ix(age,corp)
        // example: select sum(corp) from table where corp=10;
        // example: select sum(age) from table where corp=10;
        // example: select sum(id) from table where corp=10;
        // in: unGroupAggr -> indexFullScan
        // out: DingoIndexScanWithRelOp (filter, project, aggr)
        DingoRelOp dingoRelOp = call.rel(0);
        IndexFullScan indexFullScan = call.rel(1);
        Table table = Objects.requireNonNull(indexFullScan.getTable().unwrap(DingoTable.class)).getTable();

        RelOp filterRelOp = null;
        RexNode rexFilter = indexFullScan.getFilter();
        List<Column> columnNames = indexFullScan.getIndexTable().getColumns();
        List<Integer> indexSelectionList = columnNames
          .stream().map(table.columns::indexOf).collect(Collectors.toList());
        Mapping mapping = Mappings.target(indexSelectionList, table.getColumns().size());
        if (indexFullScan.getFilter() != null) {
            rexFilter = RexUtil.apply(mapping, rexFilter);
            if (rexFilter != null) {
                Expr expr = RexConverter.convert(rexFilter);
                filterRelOp = RelOpBuilder.builder()
                    .filter(expr)
                    .build();
            }
        }
        RelOp projectRelOp = null;
        if (indexFullScan.getSelection() != null) {
            int[] mappings = indexFullScan.getSelection().getMappings();
            Expr[] exprs = new Expr[mappings.length];
            for (int i = 0; i < mappings.length; i ++) {
                Column column = table.getColumns().get(mappings[i]);
                int indexIx = indexFullScan.getIndexTable().getColumns().indexOf(column);
                RexInputRef rexInputRef = new RexInputRef(indexIx,
                  indexFullScan.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER));
                exprs[i] = RexConverter.convert(rexInputRef);
            }
            projectRelOp = RelOpBuilder.builder()
                .project(exprs)
                .build();
        }

        try {
            RelOp op = RelOpBuilder.builder(filterRelOp).add(projectRelOp).add(dingoRelOp.getRelOp()).build();
            call.transformTo(
                new DingoIndexScanWithRelOp(
                    indexFullScan.getCluster(),
                    dingoRelOp.getTraitSet(),
                    dingoRelOp.getHints(),
                    indexFullScan.getTable(),
                    dingoRelOp.getRowType(),
                    op,
                    rexFilter,
                    true,
                    0,
                    (IndexTable) indexFullScan.getIndexTable(),
                    false
                )
            );
        } catch (Exception ignored) {

        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        IndexFullScanWithRelOpRule.Config INDEX_FULL_WITH_RELOP = ImmutableIndexFullScanWithRelOpRule.Config.builder()
            .description("IndexFullScanWithRelOpRule")
            .operandSupplier(b0 ->
                b0.operand(DingoRelOp.class)
                .predicate(dingoRelOp -> dingoRelOp.getRelOp() instanceof UngroupedAggregateOp)
                .oneInput(b1 ->
                    b1.operand(IndexFullScan.class)
                        .predicate(indexFullScan -> !indexFullScan.isLookup()).anyInputs()
                )
            )
            .build();

        @Override
        default IndexFullScanWithRelOpRule toRule() {
            return new IndexFullScanWithRelOpRule(this);
        }

    }
}
