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
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.SqlGenerateSeriesOperator;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static io.dingodb.calcite.type.converter.DefinitionMapper.mapToRelDataType;

public class TableGenerateSeriesFunctionNamespace extends AbstractNamespace {

    @Getter
    private final SqlBasicCall function;

    @Getter
    private final DingoRelOptTable table;

    private SqlValidatorImpl validator;

    /**
     * Creates an AbstractNamespace.
     *
     * @param validator     Validator
     * @param enclosingNode Enclosing node
     */
    public TableGenerateSeriesFunctionNamespace(SqlValidatorImpl validator, @Nullable SqlBasicCall enclosingNode) {
        super(validator, enclosingNode);
        this.validator = validator;
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

        List<SqlNode> operandList = this.function.getOperandList();
        if (function.getOperator() instanceof SqlGenerateSeriesOperator) {
            if (operandList.size() < 4) {
                throw new RuntimeException("Incorrect parameter count for generate series function");
            }
            if (operandList.get(1) == null || operandList.get(2) == null) {
                throw new IllegalArgumentException("Parameter cannot be null");
            }
            SqlIdentifier column1 = (SqlIdentifier) operandList.get(1);
            SqlIdentifier column2 = (SqlIdentifier) operandList.get(2);

            Table table = dingoTable.getTable();
            if (column1.names.size() > 1 && (
                !table.getName().equalsIgnoreCase(column1.getSimple())
                || !table.getName().equalsIgnoreCase(column2.getSimple()))) {
                throw new RuntimeException("Parameter tableName: " + column1.getSimple()
                    + " must be consistent with function tableName: " + table.getName());
            }
            Column col1 = table.getColumn(column1.getLastName());
            Column col2 = table.getColumn(column2.getLastName());
            if (col1 == null || col2 == null) {
                throw new RuntimeException("Column " + column1.getLastName() + " does not exist in table " + table.getName());
            }

            List<Column> columns = new ArrayList<>();
            Column col = Column.builder()
                .name("GENERATED_SERIES")
                .sqlTypeName(col2.getSqlTypeName())
                .type(col1.getType())
                .build();
            columns.add(col);
            if (operandList.size() == 5 && operandList.get(4) != null) {
                // The column used as the join condition must exist in the table
                // SqlBasicCall --> SqlIdentifier
                if (operandList.get(4) instanceof SqlBasicCall) {
                    SqlBasicCall call = (SqlBasicCall) operandList.get(4);
                    List<SqlNode> nodeList = call.getOperandList();
                    for (SqlNode node : nodeList) {
                        if (node instanceof SqlIdentifier) {
                            SqlIdentifier identifier = (SqlIdentifier) node;
                            if (identifier.names.size() > 1 && !table.getName().equalsIgnoreCase(identifier.getSimple())) {
                                throw new RuntimeException("Parameter tableName: " + identifier.getSimple()
                                    + " must be consistent with function tableName: " + table.getName());
                            }
                            Column column = table.getColumn(identifier.getLastName());
                            if (column == null) {
                                throw new RuntimeException("Column " + identifier.getLastName() + " does not exist in table " + table.getName());
                            }
                            columns.add(column);
                        }
                    }
                } else if (operandList.get(4) instanceof SqlIdentifier && ((SqlIdentifier) operandList.get(4)).isStar()) {
                    columns.addAll(table.getColumns());
                } else if (operandList.get(4) instanceof SqlIdentifier && !((SqlIdentifier) operandList.get(4)).isStar()) {
                    SqlIdentifier identifier = (SqlIdentifier) operandList.get(4);
                    if (identifier.names.size() > 1 && !table.getName().equalsIgnoreCase(identifier.getSimple())) {
                        throw new RuntimeException("Parameter tableName: " + identifier.getSimple()
                            + " must be consistent with function tableName: " + table.getName());
                    }
                    Column column = table.getColumn(identifier.getLastName());
                    if (column == null) {
                        throw new RuntimeException("Column " + identifier.getLastName() + " does not exist in table " + table.getName());
                    }
                    columns.add(column);
                } else {
                    throw new IllegalArgumentException("");
                }
            }

            RelDataTypeFactory typeFactory = validator.typeFactory;
            rowType = typeFactory.createStructType(
                columns.stream().map(c -> mapToRelDataType(c, typeFactory)).toList(),
                columns.stream().map(Column::getName).toList()
            );
        } else {
            throw new RuntimeException("unsupported operator type.");
        }
        return rowType;
    }

    @Override
    public @Nullable SqlNode getNode() {
        return function;
    }
}
