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

package io.dingodb.meta;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface InfoSchemaService {
    byte[] mTenants = "tenants".getBytes();
    String mTenantPrefix= "tenant";
    //byte[] mDBs = "DBs".getBytes();
    String mDBPrefix= "DB";
    String mTablePrefix= "Table";
    String TEMPLATE = "%s:%d";

    static InfoSchemaService root() {
        return InfoSchemaServiceProvider.getDefault().root();
    }

    /**
     * get global variables.
     * @return all global variables
     */
    Map<String, String> getGlobalVariables();

    void putGlobalVariable(String key, Object val);

    default byte[] tenantKey(long tenantId) {
        return TenantKey(tenantId);
    }

    default byte[] TenantKey(long tenantId) {
        String tenantIdStr = String.format(TEMPLATE, mTenantPrefix, tenantId);
        return tenantIdStr.getBytes();
    }

    default byte[] schemaKey(long schemaId) {
        return SchemaKey(schemaId);
    }

    default byte[] SchemaKey(long schemaId) {
        String schemaIdStr = String.format(TEMPLATE, mDBPrefix, schemaId);
        return schemaIdStr.getBytes();
    }

    default long parseSchemaKey(byte[] schemaKey) {
        //todo
        return 1;
    }

    default boolean isSchemaKey() {
        // todo
        return true;
    }

    default byte[] tableKey(long tableId) {
        return TableKey(tableId);
    }

    default byte[] TableKey(long tableId) {
        String tableKeyStr = String.format(TEMPLATE, mTablePrefix, tableId);
        return tableKeyStr.getBytes();
    }

    default boolean isTableKey() {
        // todo
        return true;
    }

    default long parseTableKey() {
        // todo
        return 0;
    }

    boolean checkTenantExists(byte[] tenantKey);

    boolean checkDBExists(byte[] tenant, byte[] schemaKey);

    boolean checkTableExists(byte[] schemaKey, byte[] tableKey);

    void createTableOrView(long tenantId, long schemaId, long tableId, Object table);

    void createSchema(long tenantId, long schemaId, Object schema);

    void createTenant(long tenantId, Object tenant);

    Object getTenant(long tenantId);

    List<Object> listTenant();

    Object getSchema(long tenantId, long schemaId);

    List<Object> listSchema(long tenantId);

    Object getTable(long tenantId, long schemaId, long tableId);

    List<Object> listTable(long tenantId, long schemaId);

    void dropTenant(long tenantId);
    void dropSchema(long tenantId, long schemaId);
    void dropTable(long tenantId, long schemaId, long tableId);

}
