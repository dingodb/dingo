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

import io.dingodb.common.CommonId;
import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.SchemaDiff;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.meta.Tenant;
import io.dingodb.common.mysql.MysqlByteUtil;

import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;

import java.util.List;
import java.util.Map;

public interface InfoSchemaService {
    long tenantId = TenantConstant.TENANT_ID;
    byte[] mTenants = "tenants".getBytes();
    String mTenantPrefix= "tenant";
    //byte[] mDBs = "DBs".getBytes();
    String mDBPrefix= "DB";
    String mTablePrefix= "Table";
    String mIndexPrefix= "Index";
    String mSchemaDiffPrefix = "Diff";
    String TEMPLATE = "%s:%d";
    String mSchemaVersionKey = "SchemaVersionKey";
    String nextGlobalID = "NextGlobalID";
    String mDDLJobHistoryKey = "DDLJobHistory";
    String mHistoryJobPrefix = String.format("%s:%s", DdlUtil.tenantPrefix, mDDLJobHistoryKey);

    byte[] mHistoryJobPrefixKeys = mHistoryJobPrefix.getBytes();

    String mDdlTemplate = "%s:%s:%d";

    String mSchemaVerTemplate = "%s:%s";

    String globalSchemaVer = String.format(mSchemaVerTemplate, DdlUtil.tenantPrefix, DdlUtil.DDLGlobalSchemaVersion);
    String expSchemaVer = String.format(mSchemaVerTemplate, DdlUtil.tenantPrefix, DdlUtil.DDLExpSchemaVersion);

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

    default byte[] replicaTableKey(long tableId) {
        return ReplicaTableKey(tableId);
    }

    default byte[] ReplicaTableKey(long tableId) {
        //String indexKeyStr = String.format(TEMPLATE, mReplicaTablePrefix, tableId);
        //return indexKeyStr.getBytes();
        return "replicaTable".getBytes();
    }

    default byte[] indexKey(long indexId) {
        return IndexKey(indexId);
    }

    default byte[] IndexKey(long indexId) {
        String indexKeyStr = String.format(TEMPLATE, mIndexPrefix, indexId);
        return indexKeyStr.getBytes();
    }

    default byte[] schemaDiffKey(long schemaVersion) {
        String schemaDiffKey = String.format(mDdlTemplate, DdlUtil.tenantPrefix, mSchemaDiffPrefix, schemaVersion);
        return schemaDiffKey.getBytes();
    }

    default byte[] schemaVerKey() {
        String schemaVerKey = String.format(mSchemaVerTemplate, DdlUtil.tenantPrefix, mSchemaVersionKey);
        return schemaVerKey.getBytes();
    }

    default byte[] schemaVerKeyByTenant(long tenantId) {
        String tenantKey = String.format("tenant:%d", tenantId);
        String schemaVerKey = String.format(mSchemaVerTemplate, tenantKey, mSchemaVersionKey);
        return schemaVerKey.getBytes();
    }

    default byte[] nextGlobalID() {
        String globalId = String.format(mSchemaVerTemplate, DdlUtil.tenantPrefix, nextGlobalID);
        return globalId.getBytes();
    }

    default byte[] jobIdKey(Long jobId) {
        return MysqlByteUtil.longToBytesBigEndian(jobId);
    }

    default byte[] historyJobIdKey(long jobId) {
        return (mHistoryJobPrefix + ":" + jobId).getBytes();
    }

    boolean checkTenantExists(byte[] tenantKey);

    boolean checkDBExists(byte[] tenant, byte[] schemaKey);

    boolean checkTableExists(byte[] schemaKey, byte[] tableKey);

    void createTableOrView(long schemaId, long tableId, Object table);
    default void createReplicaTable(long schemaId, long tableId, Object table) {}

    void createIndex(long schemaId, long tableId, Object index);

    void createSchema(long schemaId, SchemaInfo schema);

    default boolean createTenant(long tenantId, Tenant tenant) { return true; };

    default boolean updateTenant(long tenantId, Tenant tenant) { return true; }

    Object getTenant(long tenantId);

    List<Object> listTenant();

    Object getSchema(long schemaId);

    SchemaInfo getSchema(String schemaName);

    void updateSchema(SchemaInfo schemaInfo);

    List<SchemaInfo> listSchema();

    Object getTable(long schemaId, long tableId);
    Object getTable(long schemaId, long tableId, long tenantId);
    Object getTable(CommonId tableId);
    Object getTable(CommonId tableId, long tenantId);
    default Object getReplicaTable(long schemaId, long tableId, long replicaTableId) { return null; }

    Table getTableDef(long schemaId, long tableId);
    Table getTableDef(long schemaId, String tableName);
    Table getTableDef(long schemaId, String tableName, long tenantId);
    IndexTable getIndexDef(long tableId, long indexId);
    default IndexTable getIndexDef(long schemaId, long tableId, long indexId) {
        return null;
    }

    Object getTable(long schemaId, String tableName);
    Object getTable(long schemaId, String tableName, long tenantId);
    Object getTable(String schemaName, String tableName);
    Object getTable(long tableId);

    List<Object> listTable(long schemaId);

    default List<Object> listTable(long schemaId, long tenantId) { return null; }

    List<Object> listTable(String schemaName);

    List<Object> listIndex(long schemaId, long tableId);
    List<Object> listIndex(long schemaId, long tableId, long tenantId);
    Object getIndex(long tableId, long indexId);

    void dropTenant(long tenantId);
    void dropSchema(long schemaId);
    void dropTable(long schemaId, long tableId);
    void dropIndex(long tableId, long indexId);

    long genSchemaId();

    List<Object> scanRegions(byte[] startKey, byte[] endKey);

    int getStoreReplica();

    int getIndexReplica();

    int getDocumentReplica();

    long getSchemaVersionWithNonEmptyDiff();

    Map<String, Table> listTableDef(long schemaId);
    default Map<String, Table> listTableDef(long schemaId, long tenantId) { return null; };

    void putKvToCoordinator(String key, String val);

    void delKvFromCoordinator(String key, String keyEnd);

    List<io.dingodb.common.store.KeyValue> getByKey(String key, String keyEnd);

    Long genSchemaVersion(long step);

    void setSchemaDiff(SchemaDiff schemaDiff);
    default void delSchemaDiff(long ver) {};

    void updateTable(long schemaId, Object table);

    default void updateReplicaTable(long schemaId, long tableId, Object table) {}

    default void updateIndex(long tableId, Object index) {}

    DdlJob getHistoryDDLJob(long jobId);

    void addHistoryDDLJob(DdlJob job, boolean updateRawArgs);

    default void delHistoryDDLJob(long jobId) {};

    void prepareDone();
    default void prepareStart() {};

    boolean prepare();
    default boolean prepareStarted() { return false; }

    List<Long> genGlobalIDs(int n);

    default InfoSchema getInfoSchemaByTenantId(long tenantId) { return null; }

}
