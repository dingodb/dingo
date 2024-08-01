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

import io.dingodb.common.ddl.TableInfoCache;
import io.dingodb.common.log.LogUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.meta.ddl.InfoSchemaBuilder.bucketIdx;

@Slf4j
@Data
public class InfoSchema {
    public long schemaMetaVersion;

    public Map<String, SchemaTables> schemaMap;

    public Map<Integer, List<TableInfoCache>> sortedTablesBuckets;

    public InfoSchema() {
        schemaMetaVersion = 0;
        this.schemaMap = new ConcurrentHashMap<>();
        this.sortedTablesBuckets = new ConcurrentHashMap<>();
    }

    public Table getTable(String schemaName, String tableName) {
        tableName = tableName.toUpperCase();
        if (schemaMap.containsKey(schemaName)) {
            Map<String, Table> tableMap = schemaMap.get(schemaName).getTables();
            if (tableMap != null) {
                return tableMap.get(tableName);
            }
        }
        return null;
    }

    public boolean dropTable(String schemaName, String tableName) {
        schemaName = schemaName.toUpperCase();
        tableName = tableName.toUpperCase();
        if (getSchemaMap().containsKey(schemaName)) {
            SchemaTables schemaTables = getSchemaMap().get(schemaName);
            return schemaTables.dropTable(tableName);
        }
        return false;
    }

    public boolean putTable(String schemaName, String tableName, Table table) {
        if (getSchemaMap().containsKey(schemaName)) {
            SchemaTables schemaTables = schemaMap.get(schemaName);
            schemaTables.putTable(tableName, table);
            return true;
        }
        return false;
    }

    public Table getTable(long tableId) {
        int idx = bucketIdx(tableId);
        List<TableInfoCache> buckets = this.sortedTablesBuckets.get(idx);
        if (buckets == null) {
            return null;
        }
        TableInfoCache tableInfo
            = buckets.stream().filter(t -> t.getTableId() == tableId).findFirst().orElse(null);
        if (tableInfo == null) {
            return null;
        }
        SchemaTables schemaTables = schemaMap.get(tableInfo.getSchemaName());
        return schemaTables.getTables().get(tableInfo.getName());
    }

    public Table getIndex(long tableId, long indexId) {
        Table table = getTable(tableId);
        if (table == null || table.getIndexes() == null) {
            LogUtils.error(log, "[ddl] info schema get index: table is null or table indexes is null, tableId:{}, indexId:{}", tableId, indexId);
            return null;
        }
        return table.getIndexes().stream().filter(index -> index.getTableId().seq == indexId).findFirst().orElse(null);
    }

}
