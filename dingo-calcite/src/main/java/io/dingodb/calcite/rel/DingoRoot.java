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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import javax.annotation.Nonnull;

public class DingoRoot extends SingleRel implements DingoRel {
    public DingoRoot(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoRoot(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@Nonnull RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public <T> T accept(@Nonnull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
