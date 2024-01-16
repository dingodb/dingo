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

import io.dingodb.expr.rel.RelOp;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public class LogicalScanWithRelOp extends TableScan {
    @Getter
    protected final RelOp relOp;
    @Getter
    protected final boolean pushDown;

    protected RelDataType rowType;

    public LogicalScanWithRelOp(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        RelDataType rowType,
        RelOp relOp,
        boolean pushDown
    ) {
        super(cluster, traitSet, hints, table);
        this.relOp = relOp;
        this.pushDown = pushDown;
        this.rowType = rowType;
    }

    @Override
    public RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public @NonNull RelWriter explainTerms(@NonNull RelWriter pw) {
        super.explainTerms(pw);
        // crucial, this is how Calcite distinguish between different node with different props.
        pw.itemIf("relOp", relOp, relOp != null);
        pw.itemIf("pushDown", pushDown, pushDown);
        return pw;
    }
}
