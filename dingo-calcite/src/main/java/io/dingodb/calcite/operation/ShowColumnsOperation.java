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

package io.dingodb.calcite.operation;

import io.dingodb.calcite.grammar.dql.SqlShowColumns;
import io.dingodb.common.util.SqlLikeUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowColumnsOperation implements QueryOperation {

    @Setter
    public SqlNode sqlNode;

    private MetaService metaService;

    private String schemaName;
    private String tableName;

    private String sqlLikePattern;

    public ShowColumnsOperation(SqlNode sqlNode) {
        SqlShowColumns showColumns = (SqlShowColumns) sqlNode;
        this.schemaName = showColumns.schemaName;
        this.metaService = MetaService.root().getSubMetaService(schemaName);
        this.tableName = showColumns.tableName;
        this.sqlLikePattern = showColumns.sqlLikePattern;
    }

    @Override
    public Iterator getIterator() {
        List<Object[]> tuples = new ArrayList<>();
        List<List<String>> columnList = getColumnFields();
        for (List<String> values : columnList) {
            Object[] tuple = values.toArray();
            tuples.add(tuple);
        }
        return tuples.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Field");
        columns.add("Type");
        columns.add("Null");
        columns.add("Key");
        columns.add("Default");
        return columns;
    }

    private List<List<String>> getColumnFields() {
        Table table = metaService.getTable(tableName);
        if (table == null) {
            throw new RuntimeException("Table " + tableName + " doesn't exist");
        }

        List<Column> columns = table.getColumns();
        List<List<String>> columnList = new ArrayList<>();
        boolean haveLike = !StringUtils.isBlank(sqlLikePattern);
        for (Column column : columns) {
            if (column.getState() != 1) {
                continue;
            }
            List<String> columnValues = new ArrayList<>();

            String columnName = column.getName();
            if (haveLike && !SqlLikeUtils.like(columnName, sqlLikePattern)) {
                continue;
            }

            columnValues.add(columnName);
            String type = column.getSqlTypeName();
            if (type.equals("VARCHAR")) {
                if (column.getPrecision() > 0) {
                    type = type + "(" + column.getPrecision() + ")";
                }
            }
            columnValues.add(type);
            columnValues.add(column.isNullable() ? "YES" : "NO");
            columnValues.add(column.isPrimary() ? "PRI" : "");
            columnValues.add(column.defaultValueExpr != null ? column.defaultValueExpr : "NULL");

            columnList.add(columnValues);
        }
        return columnList;
    }
}
