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

package io.dingodb.calcite.rel.logical;

import com.google.common.collect.ImmutableList;
import io.dingodb.expr.rel.RelOp;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

/**
 * To reduce the results of partial aggregation.
 */
public class LogicalReduceAggregate extends SingleRel {
    @Getter
    protected final ImmutableList<RelHint> hints;
    @Getter
    protected final RelOp relOp;
    @Getter
    protected final RelDataType originalInputType;

    public LogicalReduceAggregate(
        RelOptCluster cluster,
        RelTraitSet traits,
        List<RelHint> hints,
        RelNode input,
        RelOp relOp,
        RelDataType originalInputType
    ) {
        super(cluster, traits, input);
        this.hints = ImmutableList.copyOf(hints);
        this.input = input;
        this.relOp = relOp;
        this.originalInputType = originalInputType;
    }

    @Override
    public @NonNull RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalReduceAggregate(
            getCluster(),
            traitSet,
            hints,
            sole(inputs),
            relOp,
            originalInputType
        );
    }

    @Override
    public @NonNull RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("relOp", relOp);
        pw.item("originalInputType", originalInputType);
        return pw;
    }
}
