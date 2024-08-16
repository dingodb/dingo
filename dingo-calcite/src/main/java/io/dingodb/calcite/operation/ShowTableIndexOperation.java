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

import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.dingodb.common.table.ColumnDefinition.HIDE_STATE;
import static io.dingodb.common.table.ColumnDefinition.NORMAL_STATE;

public class ShowTableIndexOperation extends QueryOperation {

    @Setter
    public SqlNode sqlNode;

    private MetaService metaService;

    private String tableName;

    private String schemaName;

    public ShowTableIndexOperation(SqlNode sqlNode, String tableName) {
        this.sqlNode = sqlNode;
        this.schemaName = MetaServiceUtils.getSchemaName(tableName);
        metaService = MetaService.root().getSubMetaService(schemaName);
        this.tableName = tableName.toUpperCase();
    }

    @Override
    public Iterator<Object[]> getIterator() {
        List<Object[]> tuples;
        InfoSchema is = DdlService.root().getIsLatest();
        Table table = is.getTable(schemaName, tableName);
        tuples = metaService.getTableIndexDefinitions(table.getTableId()).values().stream().filter(i -> !i.getName().equalsIgnoreCase(tableName)).map(
            index -> new Object[] {
                tableName,
                index.getName().toUpperCase(),
                index.getProperties().getProperty("indexType").toUpperCase(),
                index.getColumns().stream()
                    .filter(this::isNormal)
                    .map(ColumnDefinition::getName).collect(Collectors.toList()),
                Optional.of(new Properties())
                    .ifPresent(__ -> __.putAll(index.getProperties()))
                    .ifPresent(__ -> __.remove("indexType")).get(),
                index.getSchemaState()
            }
        ).collect(Collectors.toList());
        return tuples.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Table");
        columns.add("Key_name");
        columns.add("Index_type");
        columns.add("Column_name");
        columns.add("Parameters");
        columns.add("State");
        return columns;
    }

    private boolean isNormal(ColumnDefinition column) {
        return (column.getState() & NORMAL_STATE) == NORMAL_STATE && (column.getState() & HIDE_STATE) == 0;
    }

}
