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

package io.dingodb.calcite.executor;

import io.dingodb.calcite.grammar.dql.SqlShowIndexFromTable;
import io.dingodb.calcite.runtime.DingoResource;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ShowIndexFromTableExecutor extends QueryExecutor {
    private final String tableName;

    private final String schemaName;

    public ShowIndexFromTableExecutor(SqlShowIndexFromTable sqlShowIndexFromTable) {
        tableName = sqlShowIndexFromTable.tableName;
        schemaName = sqlShowIndexFromTable.schemaName;
    }

    @Override
    Iterator<Object[]> getIterator() {
        InfoSchema is = DdlService.root().getIsLatest();
        Table table = is.getTable(schemaName, tableName);
        if (table == null) {
            String errorKey = schemaName + "." + tableName;
            throw DingoResource.DINGO_RESOURCE.tableNotExists(errorKey).ex();
        }
        List<Object[]> primaryRes = table.keyColumns()
            .stream()
            .filter(column -> column.state == 1)
            .map(column -> getIndexCol(table, column.getName(), column.primaryKeyIndex + 1, true))
            .collect(Collectors.toList());
        primaryRes.addAll(table.getIndexes()
            .stream()
            .flatMap(index -> {
                Properties properties = index.getProperties();
                if (!properties.containsKey("indexType") || index.getSchemaState() != SchemaState.SCHEMA_PUBLIC) {
                    return null;
                }
                AtomicInteger seq = new AtomicInteger(1);
                List<Object[]> res = index.getOriginKeyList()
                    .stream()
                    .map(columnName -> getIndexCol(index, columnName, seq, index.unique))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
                if (index.getOriginWithKeyList() != null) {
                    for (String withColName : index.getOriginWithKeyList()) {
                        Object[] subItem = getIndexCol(index, withColName, seq, index.unique);
                        if (subItem != null) {
                            res.add(subItem);
                        }
                    }
                }
                return res.stream();
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList()));
        return primaryRes.iterator();
    }

    private Object[] getIndexCol(Table index, String columnName, AtomicInteger seq, boolean unique) {
        return getIndexCol(index, columnName, seq.getAndIncrement(), unique);
    }

    public Object[] getIndexCol(Table index, String columnName, int seqIndex, boolean unique) {
        Column column = index.getColumn(columnName);
        if (column == null) {
            return null;
        }
        Object[] val = new Object[16];
        val[0] = tableName;
        val[1] = unique ? "1" : "0";
        val[2] = index.getName();
        val[3] = seqIndex;
        val[4] = columnName;
        val[5] = 'A';
        val[6] = "0";
        val[7] = null;
        val[8] = null;
        val[9] = column.isNullable() ? "YES" : "NO";
        val[10] = index.getEngine();
        val[11] = column.getComment();
        val[12] = index.getComment();
        val[13] = "YES";
        val[14] = null;
        val[15] = "NO";
        return val;
    }

    @Override
    public List<String> columns() {
        List<String> indexList = new ArrayList<>();
        indexList.add("Table");
        indexList.add("Non_unique");
        indexList.add("Key_name");
        indexList.add("Seq_in_index");
        indexList.add("Column_name");
        indexList.add("Collation");
        indexList.add("Cardinality");
        indexList.add("Sub_part");
        indexList.add("Packed");
        indexList.add("Null");
        indexList.add("Index_type");
        indexList.add("Comment");
        indexList.add("Index_comment");
        indexList.add("Visible");
        indexList.add("Expression");
        indexList.add("Clustered");
        return indexList;
    }
}
