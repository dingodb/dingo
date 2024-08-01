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

package io.dingodb.meta.entity;


import io.dingodb.common.meta.SchemaInfo;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Data
public class SchemaTables {
    private SchemaInfo schemaInfo;
    private Map<String, Table> tables;

    public SchemaTables(SchemaInfo schemaInfo) {
        this.schemaInfo = schemaInfo;
        this.tables = new ConcurrentHashMap<>();
    }

    public SchemaTables(SchemaInfo schemaInfo, Map<String, Table> tables) {
        this.schemaInfo = schemaInfo;
        this.tables = tables;
    }

    public SchemaTables() {
        this.tables = new ConcurrentHashMap<>();
    }

    public boolean dropTable(String tableName) {
        this.tables.remove(tableName);
        return true;
    }

    public void putTable(String tableName, Table table) {
        this.tables.put(tableName, table);
    }

    public SchemaTables copy() {
        SchemaTables schemaTables = new SchemaTables();
        schemaTables.setSchemaInfo(schemaInfo.copy());
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            List<Column> copyColList = entry.getValue().columns
             .stream()
             .map(Column::copy)
             .collect(Collectors.toList());
            schemaTables.tables.put(entry.getKey(), entry.getValue().copyWithColumns(copyColList));
        }
        return schemaTables;
    }

}
