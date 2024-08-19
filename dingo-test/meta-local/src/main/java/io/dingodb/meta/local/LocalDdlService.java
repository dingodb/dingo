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

package io.dingodb.meta.local;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.DdlServiceProvider;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.SchemaTables;
import io.dingodb.meta.entity.Table;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;

public class LocalDdlService implements DdlService {
    LocalMetaService localMetaService = LocalMetaService.ROOT;

    public static LocalDdlService ROOT = new LocalDdlService();

    @AutoService(DdlServiceProvider.class)
    public static class Provider implements DdlServiceProvider {
        @Override
        public io.dingodb.meta.DdlService root() {
            return ROOT;
        }
    }

    @Override
    public void createSchema(String schemaName, String connId) {
        localMetaService.createSubMetaService(schemaName);
    }

    @Override
    public void dropSchema(SchemaInfo schemaInfo, String connId) {
        localMetaService.dropSubMetaService(schemaInfo.getName());
    }

    @Override
    public void createTableWithInfo(String schemaName, String tableName, @NonNull TableDefinition tableDefinition, String connId, String sql) {
        LocalMetaService ms = (LocalMetaService) localMetaService.getSubMetaService(schemaName);
        ms.createTables(tableDefinition, tableDefinition.getIndices());
        System.out.println("--->createTableInfo" + tableName);
    }

    @Override
    public void dropTable(SchemaInfo schemaInfo, Long tableId, String tableName, String connId) {
        LocalMetaService ms = (LocalMetaService) localMetaService.getSubMetaService(schemaInfo.getName());
        ms.dropTable(tableName);
    }

    @Override
    public void truncateTable(SchemaInfo schemaInfo, Table table, String connId) {
        LocalMetaService ms = (LocalMetaService) localMetaService.getSubMetaService(schemaInfo.getName());
        ms.truncateTable(table.getName(), 1);
    }

    @Override
    public void addColumn(SchemaInfo schemaInfo, Table table, Column column, String connId) {

    }

    @Override
    public void dropColumn(SchemaInfo schemaInfo, Long tableId, String column, String connId) {

    }

    @Override
    public InfoSchema getIsLatest() {
        InfoSchema infoSchema = new InfoSchema();
        Map<String, MetaService> childrenList = localMetaService.getSubMetaServices();
        childrenList.forEach((k, v) -> {
            SchemaInfo schemaInfo = new SchemaInfo(0, k, v.id().seq, SchemaState.SCHEMA_PUBLIC);
            Map<String, Table> tableMap = new HashMap<>();
            v.getTables().forEach(table -> {
                System.out.println("++++_" + table.getName() + ", schemaName:" + k);
                tableMap.put(table.getName(), table);
            });
            infoSchema.getSchemaMap().put(k, new SchemaTables(schemaInfo, tableMap));
        });
        System.out.println("--->load is latest");
        return infoSchema;
    }

    @Override
    public Table getTable(String schemaName, String tableName) {
        return null;
    }

    @Override
    public Table getTable(CommonId id) {
        return null;
    }

}
