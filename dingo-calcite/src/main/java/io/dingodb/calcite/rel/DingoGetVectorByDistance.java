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

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.dingodb.calcite.type.converter.DefinitionMapper.mapToRelDataType;

public class DingoGetVectorByDistance extends Filter implements DingoRel {
    @Getter
    protected CommonId indexTableId;

    @Getter
    protected Integer vectorIdIndex;

    @Getter
    protected Integer vectorIndex;

    @Getter
    protected final RelOptTable table;

    @Getter
    protected final List<SqlNode> operands;

    public DingoGetVectorByDistance(RelOptCluster cluster, RelTraitSet traits,
                                    RelNode child,
                                    RexNode condition,
                                    RelOptTable table,
                                    List<SqlNode> operands,
                                    Integer vectorIdIndex,
                                    Integer vectorIndex,
                                    CommonId indexTableId) {
        super(cluster, traits, child, condition);
        this.table = table;
        this.operands = operands;
        this.vectorIdIndex = vectorIdIndex;
        this.vectorIndex = vectorIndex;
        this.indexTableId = indexTableId;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new DingoGetVectorByDistance(getCluster(), traitSet, input, condition,
            table, operands, vectorIdIndex, vectorIndex, indexTableId);
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
        return getVectorRowType();
    }

    public RelDataType getVectorRowType() {
        DingoTable dingoTable = table.unwrap(DingoTable.class);
        List<Column> tableCols = dingoTable.getTable().getColumns();
        ArrayList<Column> cols = new ArrayList<>(tableCols.size() + 1);
        cols.addAll(tableCols);

        String indexTableName = "";
        // Get all index table definition
        List<IndexTable> indexes = dingoTable.getTable().getIndexes();
        for (Table index : indexes) {
            String indexType = index.getProperties().getProperty("indexType", "scalar").toString();
            // Skip if not a vector table
            if (indexType.equals("scalar")) {
                continue;
            }

            List<String> indexColumns = index.getColumns().stream().map(Column::getName).collect(Collectors.toList());
            // Skip if the vector column is not included
            if (!indexColumns.contains(((SqlIdentifier) operands.get(1)).getSimple().toUpperCase())) {
                continue;
            }

            indexTableName = index.getName();
            break;
        }

        cols.add(Column
            .builder()
            .name(indexTableName.concat("$distance"))
            .sqlTypeName("FLOAT")
            .type(DingoTypeFactory.INSTANCE.fromName("FLOAT", null, false))
            .build()
        );

        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        return typeFactory.createStructType(
            cols.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
            cols.stream().map(Column::getName).map(String::toUpperCase).collect(Collectors.toList())
        );
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
