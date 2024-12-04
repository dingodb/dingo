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

import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.utils.RelDataTypeUtils;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class LogicalDingoDiskAnnBuild extends LogicalDingoDiskAnn {

    public LogicalDingoDiskAnnBuild(RelOptCluster cluster,
                                    RelTraitSet traitSet,
                                    RexCall call,
                                    DingoRelOptTable table,
                                    List<Object> operands,
                                    @NonNull CommonId indexTableId,
                                    @NonNull Table indexTable,
                                    TupleMapping selection,
                                    RexNode filter,
                                    List<RelHint> hints
                              ) {
        super(cluster, traitSet, call, table, operands, indexTableId, indexTable, selection, filter, hints);
    }

    @Override
    public boolean deepEquals(@Nullable Object obj) {
        if (obj instanceof LogicalDingoDiskAnnBuild) {
            LogicalDingoDiskAnnBuild that = (LogicalDingoDiskAnnBuild) obj;
            boolean result = super.deepEquals(obj);
            return result && that.filter == filter
                && that.realSelection == realSelection && that.selection == selection;
        }
        return false;
    }

    @Override
    public TableFunctionScan copy(RelTraitSet traitSet,
                                  List<RelNode> inputs,
                                  RexNode rexCall,
                                  @Nullable Type elementType,
                                  RelDataType rowType,
                                  @Nullable Set<RelColumnMapping> columnMappings) {
        return new LogicalDingoDiskAnnBuild(
            getCluster(),
            traitSet,
            call, table, operands, indexTableId, indexTable, selection, filter, hints);
    }

    @Override
    public @NonNull RelWriter explainTerms(@NonNull RelWriter pw) {
        super.explainTerms(pw);
        // crucial, this is how Calcite distinguish between different node with different props.
        return pw;
    }

    @Override
    protected RelDataType deriveRowType() {
        return getSelectedType();
    }

    public RelDataType getSelectedType() {
        return RelDataTypeUtils.mapType(
            getCluster().getTypeFactory(),
            getTableType(),
            realSelection
        );
    }

    public RelDataType getTableType() {
        RelDataType relDataType = table.getRowType();
        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        builder.add(new RelDataTypeFieldImpl(
            indexTable.getName() + "$regionId",
            relDataType.getFieldCount(),
            getCluster().getTypeFactory().createSqlType(SqlTypeName.get("BIGINT"))));
        builder.add(new RelDataTypeFieldImpl(
            indexTable.getName() + "$status",
            relDataType.getFieldCount(),
            getCluster().getTypeFactory().createSqlType(SqlTypeName.get("VARCHAR"))));
        return builder.build();
    }

    public TupleType tupleType() {
        DingoTable dingoTable = table.unwrap(DingoTable.class);
        assert dingoTable != null;
        ArrayList<Column> cols = new ArrayList<>();
        cols.add(Column
            .builder()
            .name(indexTable.getName().concat("$regionId"))
            .sqlTypeName("BIGINT")
            .type(new LongType(false))
            .build());
        cols.add(Column
            .builder()
            .name(indexTable.getName().concat("$status"))
            .sqlTypeName("VARCHAR")
            .type(new StringType(false))
            .build());
        return DingoTypeFactory.tuple(cols.stream().map(Column::getType).toArray(DingoType[]::new));
    }

}
