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
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.MetaService;
import io.dingodb.partition.DingoPartitionServiceProvider;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.dingodb.common.table.ColumnDefinition.HIDE_STATE;
import static io.dingodb.common.table.ColumnDefinition.NORMAL_STATE;

public class ShowTableIndexOperation implements QueryOperation {

    @Setter
    public SqlNode sqlNode;

    private MetaService metaService;

    private String tableName;

    public ShowTableIndexOperation(SqlNode sqlNode, String tableName) {
        this.sqlNode = sqlNode;
        metaService = MetaService.root().getSubMetaService(MetaServiceUtils.getSchemaName(tableName));
        this.tableName = tableName.toUpperCase();
    }

    @Override
    public Iterator getIterator() {
        List<Object[]> tuples;
        String indexTypeKey = "indexType";
        tuples = metaService.getTableIndexDefinitions(metaService.getTableId(tableName)).values().stream().map(
            index -> new Object[] {
                tableName,
                index.getName().toUpperCase(),
                index.getProperties().getProperty(indexTypeKey).toUpperCase(),
                index.getColumns().stream().map(ColumnDefinition::getName).collect(Collectors.toList()),
                Optional.of(new Properties())
                    .ifPresent(__ -> __.putAll(index.getProperties()))
                    .ifPresent(__ -> __.remove(indexTypeKey)).get()
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
        return columns;
    }

    private boolean normal(ColumnDefinition column) {
        return (column.getState() & NORMAL_STATE) == NORMAL_STATE && (column.getState() & HIDE_STATE) == 0;
    }

}
