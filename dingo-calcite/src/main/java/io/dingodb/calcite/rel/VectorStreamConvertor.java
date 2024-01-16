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
import io.dingodb.common.CommonId;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class VectorStreamConvertor extends SingleRel implements DingoRel {

    @Getter
    private CommonId indexId;

    @Getter
    private Integer vectorIdIndex;

    @Getter
    private Table indexTableDefinition;

    @Getter
    private boolean needRoute;

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits convention root streaming empty
     * @param input   Input relational expression
     */
    public VectorStreamConvertor(RelOptCluster cluster,
                                 RelTraitSet traits,
                                 RelNode input,
                                 CommonId indexId,
                                 Integer vectorIdIndex,
                                 Table indexTableDefinition,
                                 boolean needRoute) {
        super(cluster, traits, input);
        this.indexId = indexId;
        this.vectorIdIndex = vectorIdIndex;
        this.indexTableDefinition = indexTableDefinition;
        this.needRoute = needRoute;
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new VectorStreamConvertor(
            getCluster(),
            traitSet,
            sole(inputs),
            indexId,
            vectorIdIndex,
            indexTableDefinition,
            needRoute);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return DingoCost.FACTORY.makeTinyCost();
    }

}
