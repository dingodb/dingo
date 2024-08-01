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
import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.SchemaDiff;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.store.KeyValue;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.InfoSchemaServiceProvider;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalInfoSchemaService implements InfoSchemaService {

    private static final LocalInfoSchemaService ROOT = new LocalInfoSchemaService();
    @AutoService(InfoSchemaServiceProvider.class)
    public static class Provider implements InfoSchemaServiceProvider {
        @Override
        public io.dingodb.meta.InfoSchemaService root() {
            return ROOT;
        }
    }

    @Override
    public Map<String, String> getGlobalVariables() {
        return new HashMap<>();
    }

    @Override
    public void putGlobalVariable(String key, Object val) {

    }

    @Override
    public boolean checkTenantExists(byte[] tenantKey) {
        return false;
    }

    @Override
    public boolean checkDBExists(byte[] tenant, byte[] schemaKey) {
        return false;
    }

    @Override
    public boolean checkTableExists(byte[] schemaKey, byte[] tableKey) {
        return false;
    }

    @Override
    public void createTableOrView(long schemaId, long tableId, Object table) {

    }

    @Override
    public void createIndex(long schemaId, long tableId, Object index) {

    }

    @Override
    public void createSchema(long schemaId, SchemaInfo schema) {

    }

    @Override
    public boolean createTenant(long tenantId, Object tenant) {
        return false;
    }

    @Override
    public boolean updateTenant(long tenantId, Object tenant) {
        return false;
    }

    @Override
    public Object getTenant(long tenantId) {
        return 0;
    }

    @Override
    public List<Object> listTenant() {
        return null;
    }

    @Override
    public Object getSchema(long schemaId) {
        return null;
    }

    @Override
    public SchemaInfo getSchema(String schemaName) {
        return null;
    }

    @Override
    public void updateSchema(SchemaInfo schemaInfo) {

    }

    @Override
    public List<SchemaInfo> listSchema() {
        return null;
    }

    @Override
    public Object getTable(long schemaId, long tableId) {
        return null;
    }

    @Override
    public Object getTable(CommonId tableId) {
        return null;
    }

    @Override
    public Table getTableDef(long schemaId, long tableId) {
        return null;
    }

    @Override
    public Table getTableDef(long schemaId, String tableId) {
        return null;
    }

    @Override
    public IndexTable getIndexDef(long tableId, long indexId) {
        return null;
    }

    @Override
    public Object getTable(long schemaId, String tableName) {
        return null;
    }

    @Override
    public Object getTable(String schemaName, String tableName) {
        return null;
    }

    @Override
    public Object getTable(long tableId) {
        return null;
    }

    @Override
    public List<Object> listTable(long schemaId) {
        return null;
    }

    @Override
    public List<Object> listTable(String schemaName) {
        return null;
    }

    @Override
    public List<Object> listIndex(long schemaId, long tableId) {
        return null;
    }

    @Override
    public Object getIndex(long tableId, long indexId) {
        return null;
    }

    @Override
    public void dropTenant(long tenantId) {

    }

    @Override
    public void dropSchema(long schemaId) {

    }

    @Override
    public void dropTable(long schemaId, long tableId) {

    }

    @Override
    public void dropIndex(long tableId, long indexId) {

    }

    @Override
    public long genSchemaId() {
        return 0;
    }

    @Override
    public List<Object> scanRegions(byte[] startKey, byte[] endKey) {
        return null;
    }

    @Override
    public int getStoreReplica() {
        return 3;
    }

    @Override
    public int getIndexReplica() {
        return 3;
    }

    @Override
    public long getSchemaVersionWithNonEmptyDiff() {
        return 0;
    }

    @Override
    public Map<String, Table> listTableDef(long schemaId) {
        return null;
    }

    @Override
    public void putKvToCoordinator(String key, String val) {

    }

    @Override
    public void delKvFromCoordinator(String key, String keyEnd) {

    }

    @Override
    public List<KeyValue> getByKey(String key, String keyEnd) {
        return null;
    }

    @Override
    public Long genSchemaVersion(long step) {
        return null;
    }

    @Override
    public void setSchemaDiff(SchemaDiff schemaDiff) {

    }

    @Override
    public void updateTable(long schemaId, Object table) {

    }

    @Override
    public DdlJob getHistoryDDLJob(long jobId) {
        return null;
    }

    @Override
    public void addHistoryDDLJob(DdlJob job, boolean updateRawArgs) {

    }

    @Override
    public void prepareDone() {

    }

    @Override
    public boolean prepare() {
        return false;
    }

    @Override
    public List<Long> genGlobalIDs(int n) {
        return null;
    }
}
