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

import io.dingodb.common.mysql.util.DataTimeUtils;
import io.dingodb.common.util.SqlLikeUtils;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ShowTableStatusExecutor extends QueryExecutor {

    String schema;
    String sqlLikePattern;

    public ShowTableStatusExecutor(String schema, String sqlLikePattern) {
        this.schema = schema;
        if (sqlLikePattern.contains("\\_")) {
           sqlLikePattern = sqlLikePattern.replace("\\", "");
        }
        this.sqlLikePattern = sqlLikePattern;
    }

    @Override
    public Iterator<Object[]> getIterator() {
        InfoSchema is = DdlService.root().getIsLatest();
        return is.getSchemaMap()
            .entrySet()
            .stream()
            .filter(e -> e.getKey().equalsIgnoreCase(schema))
            .flatMap(e -> {
                Collection<Table> tables = e.getValue().getTables().values();
                return tables.stream()
                    .filter(table -> (StringUtils.isBlank(sqlLikePattern) || SqlLikeUtils.like(table.getName(), sqlLikePattern)))
                    .map(table -> new Object[] {table.getName(), table.getEngine(), table.getVersion(), table.rowFormat, null, 0, 0, 16434816, 0, 0, null, DataTimeUtils.getTimeStamp(new Timestamp(table.getCreateTime())), null, null, table.getCollate(), null, null, ""})
                    .collect(Collectors.toList()).stream();
            })
            .iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Name");
        columns.add("Engine");
        columns.add("Version");
        columns.add("Row_format");
        columns.add("Rows");
        columns.add("Avg_row_length");
        columns.add("Data_length");
        columns.add("Max_data_length");
        columns.add("Index_length");
        columns.add("Data_free");
        columns.add("Auto_increment");
        columns.add("Create_time");
        columns.add("Update_time");
        columns.add("Check_time");
        columns.add("Collation");
        columns.add("Checksum");
        columns.add("Create_options");
        columns.add("Comment");
        return columns;
    }
}
