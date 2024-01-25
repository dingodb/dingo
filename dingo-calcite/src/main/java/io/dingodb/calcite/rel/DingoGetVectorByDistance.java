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

import io.dingodb.calcite.utils.RelDataTypeUtils;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class DingoGetVectorByDistance extends Filter implements DingoRel {
    @Getter
    protected CommonId indexTableId;

    protected Table indexTable;

    @Getter
    protected Integer vectorPriIdIndex;

    @Getter
    protected Integer vectorIndex;

    @Getter
    protected final RelOptTable table;

    @Getter
    protected final List<Object> operands;

    @Getter
    protected final TupleMapping selection;

    public DingoGetVectorByDistance(RelOptCluster cluster, RelTraitSet traits,
                                    RelNode child,
                                    RexNode condition,
                                    RelOptTable table,
                                    List<Object> operands,
                                    Integer vectorIdIndex,
                                    Integer vectorIndex,
                                    CommonId indexTableId,
                                    TupleMapping selection,
                                    Table indexTable) {
        super(cluster, traits, child, condition);
        this.table = table;
        this.operands = operands;
        this.vectorPriIdIndex = vectorIdIndex;
        this.vectorIndex = vectorIndex;
        this.indexTableId = indexTableId;
        this.selection = selection;
        this.indexTable = indexTable;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new DingoGetVectorByDistance(
            getCluster(),
            traitSet,
            input,
            condition,
            table, operands, vectorPriIdIndex, vectorIndex, indexTableId, selection, indexTable);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this.getInput());
        if (rowCount < 1000) {
            rowCount = 1;
        } else {
            rowCount = 10000D;
        }
        return DingoCost.FACTORY.makeCost(rowCount, 0, 0);
    }

    @Override
    protected RelDataType deriveRowType() {
        return RelDataTypeUtils.mapType(
            getCluster().getTypeFactory(),
            getTableType(),
            selection
        );
    }

    public RelDataType getTableType() {
        RelDataType relDataType = table.getRowType();
        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        builder.addAll(relDataType.getFieldList());
        builder.add(new RelDataTypeFieldImpl(
            indexTable.getName() + "$distance",
            relDataType.getFieldCount(),
            getCluster().getTypeFactory().createSqlType(SqlTypeName.get("FLOAT"))));
        return builder.build();
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
