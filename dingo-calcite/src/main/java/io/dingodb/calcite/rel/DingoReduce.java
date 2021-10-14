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
import io.dingodb.common.table.TupleMapping;
import io.dingodb.exec.aggregate.Agg;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import javax.annotation.Nonnull;

public final class DingoReduce extends SingleRel implements DingoRel {
    @Getter
    private final TupleMapping keyMapping;
    @Getter
    private final List<Agg> aggList;

    public DingoReduce(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        TupleMapping keyMapping,
        List<Agg> aggList
    ) {
        super(cluster, traits, input);
        this.keyMapping = keyMapping;
        this.aggList = aggList;
    }

    @Override
    protected RelDataType deriveRowType() {
        return input.getRowType();
    }

    @Nonnull
    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoReduce(
            getCluster(),
            traitSet,
            sole(inputs),
            this.getKeyMapping(),
            getAggList()
        );
    }

    @Override
    public <T> T accept(@Nonnull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
