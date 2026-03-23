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

import com.google.common.collect.ImmutableSet;
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

import java.util.Set;

public class DingoAggregateRule extends ConverterRule {

    private static final Set<SqlKind> UNSPLITTABLE_AGGREGATIONS = ImmutableSet.of(
        SqlKind.LISTAGG
    );

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
        // Only take over this LogicalAggregate if it contains at least one
        // aggregation kind that LogicalSplitAggregateRule cannot handle.
        boolean hasUnsplittable = rel.getAggCallList().stream()
            .anyMatch(agg -> UNSPLITTABLE_AGGREGATIONS.contains(
                agg.getAggregation().getKind()));

        if (!hasUnsplittable) {
            // All calls can be handled by LogicalSplitAggregateRule; don't interfere.
            return false;
        }

        // Additionally, exclude kinds that are fundamentally unsupported:
        // - AVG: must be decomposed to SUM/COUNT first.
        // - GROUPING: not yet supported.
        boolean hasUnsupported = rel.getAggCallList().stream()
            .anyMatch(agg -> {
                SqlKind kind = agg.getAggregation().getKind();
                if (kind == SqlKind.AVG || kind == SqlKind.GROUPING) {
                    return true;
                }
                // After AGGREGATE_EXPAND_DISTINCT_AGGREGATES, COUNT(DISTINCT)/SUM(DISTINCT)
                // go through a special two-stage path; skip them here.
                return agg.isDistinct() && (kind == SqlKind.COUNT || kind == SqlKind.SUM);
            });

        return !hasUnsupported;
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
