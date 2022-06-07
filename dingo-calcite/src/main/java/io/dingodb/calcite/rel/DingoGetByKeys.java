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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import javax.annotation.Nonnull;

@Slf4j
public final class DingoGetByKeys extends AbstractRelNode implements DingoRel {
    @Getter
    private final RelOptTable table;
    @Getter
    private final Collection<Object[]> keyTuples;
    @Getter
    private final TupleMapping selection;

    public DingoGetByKeys(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        Collection<Object[]> keyTuples,
        @Nullable TupleMapping selection
    ) {
        super(cluster, traitSet);
        this.table = table;
        this.keyTuples = keyTuples;
        this.selection = selection;
    }

    @Override
    protected RelDataType deriveRowType() {
        return table.getRowType();
    }

    @Override
    public RelOptCost computeSelfCost(@Nonnull RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = keyTuples.size();
        double cpu = rowCount + 1;
        double io = 0;
        return planner.getCostFactory().makeCost(rowCount, cpu, io);
    }

    @Nonnull
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("table", table);
        pw.item("keyTuples", keyTuples);
        pw.item("selection", selection);
        return pw;
    }

    @Override
    public <T> T accept(@Nonnull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
