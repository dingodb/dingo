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

package io.dingodb.calcite.rel.dingo;

import io.dingodb.calcite.rel.DingoRel;
import io.dingodb.calcite.rel.logical.LogicalScanWithRelOp;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import io.dingodb.expr.rel.RelOp;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public final class DingoScanWithRelOp extends LogicalScanWithRelOp implements DingoRel {
    public DingoScanWithRelOp(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        RelDataType rowType,
        RelOp relOp,
        boolean pushDown
    ) {
        super(cluster, traitSet, hints, table, rowType, relOp, pushDown);
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visitDingoScanWithRelOp(this);
    }

    @Override
    public @NonNull RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoScanWithRelOp(
            getCluster(),
            traitSet,
            hints,
            table,
            rowType,
            relOp,
            pushDown
        );
    }
}
