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
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaService;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowColumnsOperation implements QueryOperation {

    private static final String SCHEMA_NAME = "DINGO";

    @Setter
    public SqlNode sqlNode;

    private MetaService metaService;

    private String tableName;

    public ShowColumnsOperation(SqlNode sqlNode) {
        SqlShowColumns showCreateTable = (SqlShowColumns) sqlNode;
        metaService = MetaService.root().getSubMetaService(getSchemaName(showCreateTable.tableName));
        tableName = showCreateTable.tableName;
    }

    @Override
    public Iterator getIterator() {
        List<Object[]> createTable = new ArrayList<>();
        String createTableStatement = getCreateTableStatement();
        if (StringUtils.isNotBlank(createTableStatement)) {
            Object[] tuples = new Object[]{createTableStatement};
            createTable.add(tuples);
        }
        return createTable.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("create table statement");
        return columns;
    }

    private String getSchemaName(String tableName) {
        if (tableName.contains("\\.")) {
            return tableName.split("\\.")[0];
        }
        return SCHEMA_NAME;
    }

    private String getCreateTableStatement() {
        TableDefinition tableDefinition = metaService.getTableDefinition(tableName);
        if (tableDefinition == null) {
            return "";
        }

        List<ColumnDefinition> columns = tableDefinition.getColumns();

        return "";
    }
}
