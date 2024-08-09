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

package io.dingodb.store.proxy.ddl;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.DdlServiceProvider;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.entity.InfoCache;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

@Slf4j
public class DdlService implements io.dingodb.meta.DdlService {

    public static final DdlService ROOT = new DdlService();

    @AutoService(DdlServiceProvider.class)
    public static class Provider implements DdlServiceProvider {
        @Override
        public io.dingodb.meta.DdlService root() {
            return ROOT;
        }
    }

    @Override
    public void createSchema(String schemaName, String connId) {
        DdlHandler.createSchema(schemaName, connId);
    }

    @Override
    public void dropSchema(SchemaInfo schemaInfo, String connId) {
        DdlHandler.dropSchema(schemaInfo, connId);
    }

    @Override
    public void createTableWithInfo(String schemaName, String tableName, @NonNull TableDefinition tableDefinition, String connId, String sql) {
        DdlHandler.createTableWithInfo(schemaName, tableDefinition, connId, sql);
    }

    @Override
    public void dropTable(SchemaInfo schemaInfo, Long tableId, String tableName, String connId) {
        DdlHandler.dropTable(schemaInfo, tableId, tableName, connId);
    }

    @Override
    public void truncateTable(SchemaInfo schemaInfo, Table table, String connId) {
        DdlHandler.truncateTable(schemaInfo, table, connId);
    }

    @Override
    public void createIndex(String schemaName, String tableName, TableDefinition indexDef) {
        DdlHandler.createIndex(schemaName, tableName, indexDef);
    }

    @Override
    public void dropIndex(String schemaName, String tableName, String indexName) {
        DdlHandler.dropIndex(schemaName, tableName, indexName);
    }

    @Override
    public InfoSchema getIsLatest() {
        return InfoCache.infoCache.getLatest();
    }

    @Override
    public Table getTable(String schemaName, String tableName) {
        InfoSchema[] infoSchemas = InfoCache.infoCache.cache;
        for (InfoSchema is : infoSchemas) {
            if (is == null) {
                continue;
            }
            Table obj = is.getTable(schemaName, tableName);
            if (obj != null) {
                return obj;
            }
        }
        InfoSchemaService service = InfoSchemaService.root();
        Object tableObj = service.getTable(schemaName, tableName);

        LogUtils.info(log, "[ddl] ddlService getTable by name from store kv, schemaName:{},tableName:{}, tab is null:{}", schemaName, tableName, (tableObj == null));
        return (Table) tableObj;
    }

    @Override
    public Table getTable(CommonId id) {
        InfoSchema[] infoSchemas = InfoCache.infoCache.cache;
        for (InfoSchema is : infoSchemas) {
            if (is == null) {
                continue;
            }
            Table obj;
            if (id.type == CommonId.CommonType.INDEX) {
                obj = is.getIndex(id.domain, id.seq);
            } else {
                obj = is.getTable(id.seq);
            }
            if (obj != null) {
                return obj;
            }
        }
        LogUtils.error(log, "[ddl] ddlService getTable by id from store kv, id:{}", id);
        InfoSchemaService service = InfoSchemaService.root();
        if (id.type == CommonId.CommonType.INDEX) {
            return service.getTableDef(id.domain, id.seq);
        } else {
            return service.getIndexDef(id.domain, id.seq);
        }
    }
}
