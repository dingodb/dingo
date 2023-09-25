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
import io.dingodb.common.CommonId;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
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
import java.util.Map;
import java.util.stream.Collectors;

import static io.dingodb.calcite.type.converter.DefinitionMapper.mapToRelDataType;

public class TableFunctionNamespace extends AbstractNamespace {

    @Getter
    private final SqlBasicCall function;

    @Getter
    private final DingoRelOptTable table;

    @Getter
    private CommonId indexTableId;

    @Getter
    private TableDefinition indexTableDefinition;

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
        List<ColumnDefinition> tableCols = dingoTable.getTableDefinition().getColumns();
        ArrayList<ColumnDefinition> cols = new ArrayList<>(tableCols.size() + 1);
        cols.addAll(tableCols);

        List<SqlNode> operandList = this.function.getOperandList();

        if (function.getOperator() instanceof SqlFunctionScanOperator) {
            this.rowType = DefinitionMapper.mapToRelDataType(dingoTable.getTableDefinition(), validator.typeFactory);
            return rowType;
        }

        String indexTableName = "";
        // Get all index table definition
        Map<CommonId, TableDefinition> indexDefinitions = dingoTable.getIndexTableDefinitions();
        for (Map.Entry<CommonId, TableDefinition> entry : indexDefinitions.entrySet()) {
            TableDefinition indexTableDefinition = entry.getValue();

            String indexType = indexTableDefinition.getProperties().get("indexType").toString();
            // Skip if not a vector table
            if (indexType.equals("scalar")) {
                continue;
            }

            List<String> indexColumns = indexTableDefinition.getColumns().stream().map(ColumnDefinition::getName)
                .collect(Collectors.toList());
            // Skip if the vector column is not included
            if (!indexColumns.contains(((SqlIdentifier) operandList.get(1)).getSimple().toUpperCase())) {
                continue;
            }

            indexTableName = indexTableDefinition.getName();
            this.indexTableId = entry.getKey();
            this.indexTableDefinition  = indexTableDefinition;
            break;
        }

        cols.add(ColumnDefinition
            .builder()
            .name(indexTableName.concat("$distance"))
            .type("FLOAT")
            .build()
        );

        RelDataTypeFactory typeFactory = validator.typeFactory;
        RelDataType rowType = typeFactory.createStructType(
            cols.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
            cols.stream().map(ColumnDefinition::getName).map(String::toUpperCase).collect(Collectors.toList())
        );
        this.rowType = rowType;
        return rowType;
    }

    @Override
    public @Nullable SqlNode getNode() {
        return function;
    }
}
