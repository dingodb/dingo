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

package org.apache.calcite.sql.validate;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.runtime.DingoResource;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.SqlFunctionScanOperator;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.calcite.type.converter.DefinitionMapper.mapToRelDataType;

public class TableFunctionNamespace extends AbstractNamespace {

    @Getter
    private final SqlBasicCall function;

    @Getter
    private final DingoRelOptTable table;

    @Getter
    private Table index;

    public TableFunctionNamespace(SqlValidatorImpl validator, @Nullable SqlBasicCall enclosingNode) {
        super(validator, enclosingNode);
        assert enclosingNode != null;
        this.function = enclosingNode;
        ImmutableList<String> tableNames = ((SqlIdentifier) this.function.operand(0)).names;
        if (tableNames.size() < 1) {
            throw DingoResource.DINGO_RESOURCE.invalidTableName("unknown").ex();
        }
        table = (DingoRelOptTable) Parameters.nonNull(
            validator.catalogReader.getTable(tableNames),
            () -> DingoResource.DINGO_RESOURCE.unknownTable(tableNames.get(tableNames.size() - 1)).ex()
        );
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        DingoTable dingoTable = table.unwrap(DingoTable.class);
        List<Column> tableCols = dingoTable.getTable().getColumns();
        ArrayList<Column> cols = new ArrayList<>(tableCols.size() + 1);
        cols.addAll(tableCols);

        List<SqlNode> operandList = this.function.getOperandList();

        if (function.getOperator() instanceof SqlFunctionScanOperator) {
            this.rowType = DefinitionMapper.mapToRelDataType(dingoTable.getTable(), validator.typeFactory);
            return rowType;
        }

        if (operandList.size() < 4) {
            throw new RuntimeException("Incorrect parameter count for vector function.");
        }
        SqlIdentifier columnIdentifier = (SqlIdentifier) operandList.get(1);
        // Get all index table definition
        Table table = dingoTable.getTable();

        this.index = getVectorIndexTable(table, columnIdentifier.getSimple().toUpperCase());
        cols.add(Column
            .builder()
            .name(index.getName().concat("$distance"))
            .sqlTypeName("FLOAT")
            .type(new FloatType(false))
            .precision(-1)
            .scale(-2147483648)
            .build()
        );

        RelDataTypeFactory typeFactory = validator.typeFactory;
        RelDataType rowType = typeFactory.createStructType(
            cols.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
            cols.stream().map(Column::getName).map(String::toUpperCase).collect(Collectors.toList())
        );
        this.rowType = rowType;
        return rowType;
    }

    @Override
    public @Nullable SqlNode getNode() {
        return function;
    }

    public static IndexTable getVectorIndexTable(Table table, String vectorColName) {
        for (IndexTable index : table.getIndexes()) {
            if (!index.getIndexType().isVector) {
                continue;
            }

            List<String> indexColumns = index.getColumns().stream().map(Column::getName).collect(Collectors.toList());
            // Skip if the vector column is not included
            if (!indexColumns.contains(vectorColName)) {
                continue;
            }

            return index;
        }
        throw new RuntimeException(vectorColName + " vector not found.");
    }
}
