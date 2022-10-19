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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public final class DingoPartModify extends SingleRel implements DingoRel {
    @Getter
    private final RelOptTable table;
    @Getter
    private final TableModify.Operation operation;
    @Getter
    private final List<String> updateColumnList;
    @Getter
    private final List<RexNode> sourceExpressionList;

    public DingoPartModify(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        RelOptTable table,
        TableModify.Operation operation,
        List<String> updateColumnList,
        List<RexNode> sourceExpressionList
    ) {
        super(cluster, traits, input);
        this.table = table;
        this.operation = operation;
        this.updateColumnList = updateColumnList;
        this.sourceExpressionList = sourceExpressionList;
    }

    @Override
    public @NonNull RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoPartModify(
            getCluster(),
            traitSet,
            sole(inputs),
            table,
            getOperation(),
            getUpdateColumnList(),
            getSourceExpressionList()
        );
    }

    @Override
    public @NonNull RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("table", table.getQualifiedName());
        pw.item("operation", operation);
        pw.item("updateColumnList", updateColumnList);
        pw.item("sourceExpressionList", sourceExpressionList);
        return pw;
    }

    @Override
    public RelDataType deriveRowType() {
        return RelOptUtil.createDmlRowType(SqlKind.INSERT, getCluster().getTypeFactory());
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
