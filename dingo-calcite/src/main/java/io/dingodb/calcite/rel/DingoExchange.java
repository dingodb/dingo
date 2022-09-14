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
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import javax.annotation.Nonnull;

public final class DingoExchange extends SingleRel implements DingoRel {
    @Getter
    private final boolean root;

    public DingoExchange(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        this(cluster, traits, input, false);
    }

    public DingoExchange(RelOptCluster cluster, RelTraitSet traits, RelNode input, boolean root) {
        super(cluster, traits, input);
        this.root = root;
    }

    @Nonnull
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.itemIf("root", root, root);
        return pw;
    }

    @Nonnull
    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoExchange(getCluster(), traitSet, AbstractRelNode.sole(inputs), root);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@Nonnull RelOptPlanner planner, @Nonnull RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(getInput());
        return planner.getCostFactory().makeCost(rowCount, rowCount + 1.0, rowCount);
    }

    @Override
    public <T> T accept(@Nonnull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
