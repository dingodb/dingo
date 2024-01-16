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

import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.meta.entity.Column;
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
        this.function = enclosingNode;
        table = (DingoRelOptTable) validator.catalogReader.getTable(((SqlIdentifier) this.function.operand(0)).names);
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        if (table == null) {
            throw new RuntimeException("Table is not exist: "
                + ((SqlIdentifier) this.function.operand(0)).names.get(0));
        }
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
        String indexTableName = "";
        SqlIdentifier columnIdentifier = (SqlIdentifier) operandList.get(1);
        // Get all index table definition
        Table table = dingoTable.getTable();
        for (Table index : table.getIndexes()) {
            String indexType = index.getProperties().get("indexType").toString();
            // Skip if not a vector table
            if (indexType.equalsIgnoreCase("scalar")) {
                continue;
            }

            List<String> indexColumns = index.getColumns().stream().map(Column::getName).collect(Collectors.toList());
            // Skip if the vector column is not included
            if (!indexColumns.contains(columnIdentifier.getSimple().toUpperCase())) {
                continue;
            }

            this.index = index;
            break;
        }

        if (index == null) {
            throw new RuntimeException(columnIdentifier.getSimple() + " vector not found.");
        }
        cols.add(Column
            .builder()
            .name(indexTableName.concat("$distance"))
            .sqlTypeName("FLOAT")
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
}
