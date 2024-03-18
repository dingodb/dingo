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
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class DingoGetByIndex extends LogicalDingoTableScan implements DingoRel {
    @Getter
    protected final Map<CommonId, Set> indexSetMap;
    @Getter
    protected final Map<CommonId, Table> indexTdMap;

    @Getter
    private final boolean isUnique;

    public DingoGetByIndex(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        RexNode filter,
        @Nullable TupleMapping selection,
        boolean isUnique,
        Map<CommonId, Set> indexSetMap,
        Map<CommonId, Table> indexTdMap
    ) {
        super(cluster, traitSet, hints, table, filter, selection);
        this.indexSetMap = indexSetMap;
        this.indexTdMap = indexTdMap;
        this.isUnique = isUnique;
    }

    public DingoGetByIndex(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        RexNode filter,
        @Nullable TupleMapping selection,
        boolean isUnique,
        Map<CommonId, Set> indexSetMap,
        Map<CommonId, Table> indexTdMap,
        boolean forDml
    ) {
        super(cluster, traitSet, hints, table, filter, selection, null, null, null, false, forDml);
        this.indexSetMap = indexSetMap;
        this.indexTdMap = indexTdMap;
        this.isUnique = isUnique;
    }

    @Override
    public @NonNull RelWriter explainTerms(@NonNull RelWriter pw) {
        super.explainTerms(pw);
        pw.item("points", indexSetMap);
        return pw;
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
