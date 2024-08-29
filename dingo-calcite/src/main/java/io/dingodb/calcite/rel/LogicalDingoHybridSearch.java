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
import io.dingodb.common.table.HybridSearchTable;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import lombok.Setter;
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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalDingoHybridSearch extends TableFunctionScan {

    public final static String NAME_ID = HybridSearchTable.getColumns().get(0);
    public final static String NAME_RANK_HYBRID = HybridSearchTable.getColumns().get(1);
    @Getter
    private final RexCall call;
    @Getter
    private final DingoRelOptTable table;
    @Getter
    private final List<Object> operands;
    @Getter
    private final CommonId documentIndexTableId;
    @Getter
    private final Table documentIndexTable;
    @Getter
    private final CommonId vectorIndexTableId;
    @Getter
    private final Table vectorIndexTable;
    @Getter
    protected final TupleMapping selection;
    @Getter
    protected TupleMapping realSelection;

    @Getter
    protected final RexNode filter;

    @Getter
    @Setter
    protected boolean forDml;

    @Getter
    public List<RelHint> hints;

    public LogicalDingoHybridSearch(RelOptCluster cluster,
                                    RelTraitSet traitSet,
                                    RexCall call,
                                    DingoRelOptTable table,
                                    List<Object> operands,
                                    @NonNull CommonId documentIndexTableId,
                                    @NonNull Table documentIndexTable,
                                    @NonNull CommonId vectorIndexTableId,
                                    @NonNull Table vectorIndexTable,
                                    TupleMapping selection,
                                    RexNode filter,
                                    List<RelHint> hints
                              ) {
        super(cluster, traitSet, Collections.emptyList(), call, null, call.type, null);
        this.call = call;
        this.table = table;
        this.operands = operands;
        this.documentIndexTableId = documentIndexTableId;
        this.documentIndexTable = documentIndexTable;
        this.vectorIndexTableId = vectorIndexTableId;
        this.vectorIndexTable = vectorIndexTable;
        this.filter = filter;
        this.rowType = null;
        this.realSelection = selection;
        this.hints = hints;
        DingoTable dingoTable = table.unwrap(DingoTable.class);
        if (selection != null) {
            if (forDml) {
                this.selection = selection;
            } else {
                List<Integer> mappingList = new ArrayList<>();
                for (int index : selection.getMappings()) {
                    if (index < HybridSearchTable.getColumns().size()) {
                        //if (dingoTable.getTable().getColumns().get(index).getState() == 1) {
                            mappingList.add(index);
                        //}
                    } else {
                        mappingList.add(index);
                    }
                }
                this.selection = TupleMapping.of(mappingList);
            }
        } else {
            assert dingoTable != null;
            List<Integer> mapping = HybridSearchTable
                .getColumns()
                .stream()
                .map(col -> HybridSearchTable.columns.indexOf(col))
                .collect(Collectors.toList());
            this.selection = TupleMapping.of(mapping);
        }
    }

    @Override
    public boolean deepEquals(@Nullable Object obj) {
        if (obj instanceof LogicalDingoHybridSearch) {
            LogicalDingoHybridSearch that = (LogicalDingoHybridSearch) obj;
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
        return new LogicalDingoHybridSearch(
            getCluster(),
            traitSet,
            call, table, operands, documentIndexTableId, documentIndexTable, vectorIndexTableId, vectorIndexTable, selection, filter, hints);
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
        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        builder.add(new RelDataTypeFieldImpl(
            NAME_ID,
            HybridSearchTable.INDEX_ID,
            getCluster().getTypeFactory().createSqlType(SqlTypeName.get(HybridSearchTable.TYPE_ID))));
        builder.add(new RelDataTypeFieldImpl(
            NAME_RANK_HYBRID,
            HybridSearchTable.INDEX_RANK_HYBRID,
            getCluster().getTypeFactory().createSqlType(SqlTypeName.get(HybridSearchTable.TYPE_RANK_HYBRID))));
        return builder.build();
    }

    public TupleType tupleType() {
        DingoTable dingoTable = table.unwrap(DingoTable.class);
        assert dingoTable != null;
        ArrayList<Column> cols = new ArrayList<>(2);
        cols.add(Column
            .builder()
            .name(NAME_ID)
            .sqlTypeName(HybridSearchTable.TYPE_ID)
            .type(new LongType(false))
            .build()
        );

        cols.add(Column
            .builder()
            .name(NAME_RANK_HYBRID)
            .sqlTypeName(HybridSearchTable.TYPE_RANK_HYBRID)
            .type(new FloatType(false))
            .precision(-1)
            .scale(-2147483648)
            .build()
        );
        return DingoTypeFactory.tuple(cols.stream().map(Column::getType).toArray(DingoType[]::new));
    }

}
