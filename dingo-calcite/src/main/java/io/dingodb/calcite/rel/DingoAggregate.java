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

package io.dingodb.calcite.rel;

import io.dingodb.calcite.visitor.DingoRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public final class DingoAggregate extends Aggregate implements DingoRel {
    public DingoAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelNode input,
        ImmutableBitSet groupSet,
        @Nullable List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        // In `Aggregate`, type checks were done but with `assert`, which is not effective in production.
        for (AggregateCall aggCall : aggCalls) {
            SqlKind aggKind = aggCall.getAggregation().getKind();
            if (aggKind == SqlKind.SUM || aggKind == SqlKind.SUM0) {
                if (aggCall.type.getFamily() != SqlTypeFamily.NUMERIC) {
                    throw new IllegalArgumentException(
                        "Aggregation function \"" + aggKind + "\" requires numerical input but \""
                            + aggCall.type + "\" was given."
                    );
                }
            }
        }
    }

    @Override
    public @NonNull DingoAggregate copy(
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        @Nullable List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        return new DingoAggregate(getCluster(), traitSet, getHints(), input, groupSet, groupSets, aggCalls);
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
