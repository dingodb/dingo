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

import io.dingodb.calcite.grammar.dql.SqlShowCreateTable;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import lombok.Setter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowCreateTableOperation implements QueryOperation {

    @Setter
    public SqlNode sqlNode;

    private MetaService metaService;

    private String schemaName;
    private String tableName;

    public ShowCreateTableOperation(SqlNode sqlNode, String defaultSchemaName) {
        SqlShowCreateTable showCreateTable = (SqlShowCreateTable) sqlNode;
        SqlIdentifier tableIdentifier = showCreateTable.tableIdentifier;
        if (tableIdentifier.names.size() == 1) {
            this.schemaName = defaultSchemaName;
            tableName = tableIdentifier.names.get(0);
        } else if (tableIdentifier.names.size() == 2) {
            this.schemaName = tableIdentifier.names.get(0);
            tableName = tableIdentifier.names.get(1);
        }
        metaService = MetaService.root().getSubMetaService(schemaName);
    }

    @Override
    public Iterator getIterator() {
        List<Object[]> createTableList = new ArrayList<>();
        String createTable = getCreateTable();
        if (StringUtils.isNotBlank(createTable)) {
            Object[] tuples = new Object[]{tableName, createTable};
            createTableList.add(tuples);
        }

        return createTableList.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Table");
        columns.add("Create Table");
        return columns;
    }

    private String getCreateTable() {
        Table table = metaService.getTable(tableName);
        if (table == null) {
            throw new RuntimeException("Table " + tableName + " doesn't exist");
        }
        return table.getCreateSql();
    }
}
