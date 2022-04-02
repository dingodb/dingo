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
import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.rel.DingoCoalesce;
import io.dingodb.calcite.rel.DingoExchangeRoot;
import io.dingodb.calcite.rel.DingoReduce;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.aggregate.Agg;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DingoAggregateRule extends RelRule<DingoAggregateRule.Config> {
    protected DingoAggregateRule(Config config) {
        super(config);
    }

    @Nonnull
    private static TupleMapping getAggKeys(@Nonnull Aggregate rel) {
        return TupleMapping.of(
            rel.getGroupSet().asList().stream()
                .mapToInt(Integer::intValue)
                .toArray()
        );
    }

    private static List<Agg> getAggList(@Nonnull Aggregate rel) {
        return rel.getAggCallList().stream()
            .map(c -> AggFactory.getAgg(
                c.getAggregation().getKind(),
                c.getArgList(),
                TupleSchema.fromRelDataType(rel.getInput().getRowType())
            ))
            .collect(Collectors.toList());
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        Aggregate rel = call.rel(0);
        // AVG must be transformed to SUM/COUNT before.
        if (rel.getAggCallList().stream().anyMatch(agg -> agg.getAggregation().getKind() == SqlKind.AVG)) {
            return;
        }
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet rootTraits = rel.getTraitSet().replace(DingoConventions.ROOT);
        TupleMapping keyMapping = getAggKeys(rel);
        List<Agg> aggList = getAggList(rel);
        call.transformTo(
            new DingoReduce(
                cluster,
                rootTraits,
                new DingoCoalesce(
                    cluster,
                    rootTraits,
                    new DingoExchangeRoot(
                        cluster,
                        rel.getTraitSet().replace(DingoConventions.PARTITIONED),
                        new DingoAggregate(
                            cluster,
                            rel.getTraitSet().replace(DingoConventions.DISTRIBUTED),
                            convert(rel.getInput(), DingoConventions.DISTRIBUTED),
                            keyMapping,
                            aggList,
                            rel.getRowType()
                        )
                    )
                ),
                keyMapping,
                aggList
            )
        );
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = EMPTY
            .withOperandSupplier(b0 ->
                b0.operand(Aggregate.class).trait(Convention.NONE).anyInputs()
            )
            .withDescription("DingoAggregateRule")
            .as(Config.class);

        @Override
        default DingoAggregateRule toRule() {
            return new DingoAggregateRule(this);
        }
    }
}
