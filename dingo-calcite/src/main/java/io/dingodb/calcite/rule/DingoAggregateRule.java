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

import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DingoAggregateRule extends ConverterRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalAggregate.class,
            DingoAggregateRule::match,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoAggregateRule"
        )
        .withRuleFactory(DingoAggregateRule::new);

    protected DingoAggregateRule(Config config) {
        super(config);
    }

    public static boolean match(@NonNull LogicalAggregate rel) {
        return rel.getAggCallList().stream().noneMatch(agg -> {
            SqlKind kind = agg.getAggregation().getKind();
            // AVG must be transformed to SUM/COUNT before.
            // TODO: GROUPING is not supported, maybe it is useful.
            if (kind == SqlKind.AVG || kind == SqlKind.GROUPING) {
                return true;
            }
            // After apply `CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES`, the sql: `select count(distinct a) from t`
            // will be transformed to two rules:
            // 1. aggregate with distinct(AggregateCall List is empty)
            // 2. aggregate with count(AggregateCall List contains COUNT, SUM, AVG...)
            // So, In this case, the origin aggregate and distinct should be ignored.
            return agg.isDistinct() && (kind == SqlKind.COUNT || kind == SqlKind.SUM);
        });
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        LogicalAggregate agg = (LogicalAggregate) rel;
        RelTraitSet traits = agg.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.ROOT);
        return new DingoAggregate(
            agg.getCluster(),
            traits,
            agg.getHints(),
            convert(agg.getInput(), traits),
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList()
        );
    }
}
