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

import io.dingodb.calcite.type.RelDataTypeUtils;
import io.dingodb.common.type.TupleMapping;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import javax.annotation.Nonnull;

public class LogicalDingoTableScan extends TableScan {
    @Getter
    protected final RexNode filter;
    @Getter
    protected final TupleMapping selection;

    public LogicalDingoTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        @Nullable RexNode filter,
        @Nullable TupleMapping selection
    ) {
        super(cluster, traitSet, hints, table);
        this.filter = filter;
        this.selection = selection;
    }

    @Override
    public RelDataType deriveRowType() {
        return RelDataTypeUtils.mapType(getCluster().getTypeFactory(), table.getRowType(), selection);
    }

    @Nonnull
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        // crucial, this is how Calcite distinguish between different node with different props.
        pw.itemIf("filter", filter, filter != null);
        pw.itemIf("selection", selection, selection != null);
        return pw;
    }
}
