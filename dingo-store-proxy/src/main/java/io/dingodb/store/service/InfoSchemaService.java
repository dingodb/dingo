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

package io.dingodb.store.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.dingodb.common.CommonId;
import io.dingodb.common.codec.CodecKvUtil;
import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.SchemaDiff;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.meta.Tenant;
import io.dingodb.common.mysql.scope.GlobalVariable;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.meta.InfoSchemaServiceProvider;
import io.dingodb.meta.ddl.InfoSchemaBuilder;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.VersionService;
import io.dingodb.sdk.service.entity.Message;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.StoreState;
import io.dingodb.sdk.service.entity.common.StoreType;
import io.dingodb.sdk.service.entity.coordinator.CreateIdsRequest;
import io.dingodb.sdk.service.entity.coordinator.CreateIdsResponse;
import io.dingodb.sdk.service.entity.coordinator.GetStoreMapRequest;
import io.dingodb.sdk.service.entity.coordinator.GetStoreMapResponse;
import io.dingodb.sdk.service.entity.coordinator.IdEpochType;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionsRequest;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionsResponse;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.EntityType;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.sdk.service.entity.version.DeleteRangeRequest;
import io.dingodb.sdk.service.entity.version.DeleteRangeResponse;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.sdk.service.entity.version.PutRequest;
import io.dingodb.sdk.service.entity.version.RangeRequest;
import io.dingodb.sdk.service.entity.version.RangeResponse;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.store.proxy.meta.MetaService;
import io.dingodb.store.proxy.service.TsoService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static io.dingodb.common.mysql.InformationSchemaConstant.GLOBAL_VAR_PREFIX_BEGIN;
import static io.dingodb.common.mysql.InformationSchemaConstant.GLOBAL_VAR_PREFIX_END;
import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class InfoSchemaService implements io.dingodb.meta.InfoSchemaService {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TxStructure txn;
    private VersionService versionService;
    Set<Location> coordinators;
    private static final long tenantId = TenantConstant.TENANT_ID;
    public static final InfoSchemaService ROOT = new InfoSchemaService();

    private final byte[] genSchemaVerKey = CodecKvUtil.encodeStringDataKey(schemaVerKey());

    @AutoService(InfoSchemaServiceProvider.class)
    public static class Provider implements InfoSchemaServiceProvider {
        @Override
        public io.dingodb.meta.InfoSchemaService root() {
            return ROOT;
        }
    }

    public InfoSchemaService() {
        this(0L);
    }

    public InfoSchemaService(Long startTs) {
        this.coordinators = Services.parse(Configuration.coordinators());
        this.versionService = Services.versionService(coordinators);
        this.txn = new TxStructure(startTs);
    }

    @Override
    public Map<String, String> getGlobalVariables() {
        byte[] startKey = GLOBAL_VAR_PREFIX_BEGIN.getBytes(UTF_8);
        byte[] endKey = GLOBAL_VAR_PREFIX_END.getBytes(UTF_8);
        startKey = CodecKvUtil.encodeStringDataKey(startKey);
        endKey = CodecKvUtil.encodeStringDataKey(endKey);
        List<byte[]> varList = this.txn.ddlRange(startKey, endKey);
        Map<String, String> variableMap = new LinkedHashMap<>();
        for (byte[] bytes : varList) {
            GlobalVariable globalVariable = (GlobalVariable) getObjFromBytes(bytes, GlobalVariable.class);
            variableMap.put(globalVariable.getKey(), globalVariable.getValue());
        }
        return variableMap;
    }


    @Override
    public void putGlobalVariable(String key, Object val) {
        String resourceKey = GLOBAL_VAR_PREFIX_BEGIN + key;
        GlobalVariable globalVariable = GlobalVariable
            .builder()
            .key(key)
            .value(val.toString())
            .build();
        byte[] valBytes = getBytesFromObj(globalVariable);
        this.txn.ddlPut(resourceKey.getBytes(UTF_8), valBytes);
    }

    @Override
    public boolean checkTenantExists(byte[] tenantKey) {
        byte[] val = txn.hGet(mTenants, tenantKey);
        return val != null;
    }

    @Override
    public boolean checkDBExists(byte[] tenantKey, byte[] schemaKey) {
        byte[] val = txn.hGet(tenantKey, schemaKey);
        return val != null;
    }

    public boolean checkSchemaNameExists(String schemaName) {
        List<SchemaInfo> schemaInfoList = listSchema();
        return schemaInfoList.stream()
            .anyMatch(schemaInfo -> schemaInfo.getName().equalsIgnoreCase(schemaName));
    }

    @Override
    public boolean checkTableExists(byte[] schemaKey, byte[] tableKey) {
        byte[] value = txn.hGet(schemaKey, tableKey);
        return value != null;
    }

    @Override
    public void createTableOrView(long schemaId, long tableId, Object table) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        if (!checkDBExists(tenantKey, schemaKey)) {
            throw new RuntimeException("schema is null");
        }
        byte[] tableKey = tableKey(tableId);
        byte[] val = getBytesFromObj(table);
        txn.hInsert(schemaKey, tableKey, val);
    }

    public void createReplicaTable(long schemaId, long tableId, Object table) {
        byte[] tableKey = tableKey(tableId);
        TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) table;
        byte[] replicaKey = indexKey(tableDefinitionWithId.getTableId().getEntityId());
        byte[] val = getBytesFromObj(table);
        txn.hInsert(tableKey, replicaKey, val);
    }

    @Override
    public void createIndex(long schemaId, long tableId, Object index) {
        byte[] tableKey = tableKey(tableId);
        TableDefinitionWithId indexWithId = (TableDefinitionWithId) index;

        byte[] indexKey = indexKey(indexWithId.getTableId().getEntityId());
        byte[] val = getBytesFromObj(index);
        txn.hInsert(tableKey, indexKey, val);
    }

    @SneakyThrows
    private byte[] getBytesFromObj(Object table) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        if (table instanceof Message) {
            CodedOutputStream out = CodedOutputStream.newInstance(outputStream);
            ((Message) table).write(out);
            out.flush();
        } else {
            try {
                objectMapper.writeValue(outputStream, table);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return outputStream.toByteArray();
    }

    private Object getObjFromBytes(byte[] val, Class type) {
        try {
            if (type.newInstance() instanceof Message) {
                CodedInputStream inputStream = CodedInputStream.newInstance(new ByteArrayInputStream(val));
                Message message = (Message) type.newInstance();
                message.read(inputStream);
                return message;
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        try {
            return objectMapper.readValue(val, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createSchema(long schemaId, SchemaInfo schema) {
        byte[] tenantKey = tenantKey(tenantId);
        if (!checkTenantExists(tenantKey)) {
            throw new RuntimeException("tenant is null");
        }
        byte[] schemaKey = schemaKey(schemaId);
        if (checkSchemaNameExists(schema.getName())) {
            return;
        }
        schema.setTenantId(tenantId);
        byte[] val = getBytesFromObj(schema);
        txn.hInsert(tenantKey, schemaKey, val);
    }

    @Override
    public boolean createTenant(long tenantId, Tenant tenant) {
        byte[] tenantKey = tenantKey(tenantId);
        if (checkTenantExists(tenantKey)) {
            return false;
        }
        byte[] val = getBytesFromObj(tenant);
        txn.hInsert(mTenants, tenantKey, val);
        MetaService.ROOT.createTenant(tenant);
        return true;
    }

    @Override
    public boolean updateTenant(long tenantId, Tenant tenant) {
        byte[] tenantKey = tenantKey(tenantId);
        if (!checkTenantExists(tenantKey)) {
            return false;
        }
        byte[] val = getBytesFromObj(tenant);
        this.txn.hPut(mTenants, tenantKey, val);
        MetaService.ROOT.updateTenant(tenant);
        return true;
    }

    @Override
    public Object getTenant(long tenantId) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] val = txn.hGet(mTenants, tenantKey);
        if (val == null) {
            return null;
        }
        return getObjFromBytes(val, Tenant.class);
    }

    @Override
    public List<Object> listTenant() {
        byte[] dataPrefix = CodecKvUtil.hashDataKeyPrefix(mTenants);
        byte[] end = CodecKvUtil.hashDataKeyPrefixUpperBound(dataPrefix);
        List<byte[]> valueList = txn.mRange(dataPrefix, end);
        if (!valueList.isEmpty()) {
            return valueList.stream().map(val -> getObjFromBytes(val, Tenant.class)).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    @Override
    public Object getSchema(long schemaId) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        byte[] val = txn.hGet(tenantKey, schemaKey);
        if (val == null) {
            return null;
        }
        return getObjFromBytes(val, SchemaInfo.class);
    }

    @Override
    public SchemaInfo getSchema(String schemaName) {
        List<SchemaInfo> schemaList = listSchema();
        return schemaList.stream()
            .filter(schemaInfo1 -> schemaInfo1.getName().equalsIgnoreCase(schemaName))
            .findFirst().orElse(null);
    }

    @Override
    public void updateSchema(SchemaInfo schemaInfo) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] key = schemaKey(schemaInfo.getSchemaId());

        byte[] val = getBytesFromObj(schemaInfo);

        this.txn.hPut(tenantKey, key, val);
    }

    @Override
    public List<SchemaInfo> listSchema() {
        byte[] tenantKey = tenantKey(tenantId);

        return getSchemaInfos(tenantKey);
    }

    public List<SchemaInfo> listSchema(long tenantId) {
        byte[] tenantKey = tenantKey(tenantId);

        return getSchemaInfos(tenantKey);
    }

    @NonNull
    private List<SchemaInfo> getSchemaInfos(byte[] tenantKey) {
        List<byte[]> valueList = txn.hGetAll(tenantKey);
        if (!valueList.isEmpty()) {
            return valueList
                .stream()
                .map(val -> getObjFromBytes(val, SchemaInfo.class)).map(object -> (SchemaInfo) object)
                .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    @Override
    public Object getTable(long schemaId, long tableId) {
        return getTable(schemaId, tableId, tenantId);
    }

    @Override
    public Object getTable(long schemaId, long tableId, long tenantId) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        if (!checkDBExists(tenantKey, schemaKey)) {
            throw new RuntimeException("schema is null, schemaId:" + schemaId);
        }
        byte[] tableKey = tableKey(tableId);
        byte[] val = txn.hGet(schemaKey, tableKey);
        if (val == null) {
            return null;
        }
        return getObjFromBytes(val, TableDefinitionWithId.class);
    }

    @Override
    public Object getTable(CommonId tableId) {
        return getTable(tableId,-1);
    }

    @Override
    public Object getTable(CommonId tableId, long tenantId) {
        if (tableId.type == CommonId.CommonType.TABLE) {
            return tenantId != -1 ? getTable(tableId.domain, tableId.seq, tenantId)
                : getTable(tableId.domain, tableId.seq);
        } else if (tableId.type == CommonId.CommonType.INDEX) {
            return getIndex(tableId.domain, tableId.seq);
        }
        return null;
    }

    @Override
    public Object getTable(long schemaId, String tableName) {
        return getTable(schemaId, tableName, tenantId);
    }

    @Override
    public Object getTable(long schemaId, String tableName, long tenantId) {
        List<Object> tableList = listTable(schemaId, tenantId);
        return tableList.stream().map(object -> (TableDefinitionWithId)object)
            .filter(tableDefinitionWithId -> tableDefinitionWithId.getTableDefinition()
                .getName().equalsIgnoreCase(tableName))
            .findFirst().orElse(null);
    }

    @Override
    public Object getTable(String schemaName, String tableName) {
        SchemaInfo schemaInfo = getSchema(schemaName);
        if (schemaInfo == null) {
            return null;
        }
        return getTable(schemaInfo.getSchemaId(), tableName);
    }

    @Override
    public Object getTable(long tableId) {
        List<SchemaInfo> schemaList = listSchema();
        return schemaList.stream()
            .map(schemaInfo -> listTable(schemaInfo.getSchemaId()))
            .map(tableList -> tableList.stream().filter(object -> {
                TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) object;
                return tableDefinitionWithId.getTableId().getEntityId() == tableId;
            }).findAny().orElse(null))
            .filter(Objects::nonNull)
            .findFirst().orElse(null);
    }

    @Override
    public Object getReplicaTable(long schemaId, long tableId, long replicaTableId) {
        List<Object> withIdList = listIndex(schemaId, tableId);
        return withIdList.stream()
            .filter(obj -> {
                TableDefinitionWithId withId = (TableDefinitionWithId) obj;
                return withId.getTableDefinition().getName().equalsIgnoreCase(DdlUtil.ddlTmpTableName);
            }).findFirst()
            .orElse(null);
    }

    @Override
    public List<Object> getReplicaIndex(long schemaId, long tableId) {
        List<Object> withIdList = listIndex(schemaId, tableId);
        return withIdList.stream()
            .filter(obj -> {
                TableDefinitionWithId withId = (TableDefinitionWithId) obj;
                return withId.getTableDefinition().getName().startsWith(DdlUtil.ddlTmpIndexName);
            }).collect(Collectors.toList());
    }

    @Override
    public Table getTableDef(long schemaId, long tableId) {
        TableDefinitionWithId tableWithId = (TableDefinitionWithId) getTable(schemaId, tableId);
        if (tableWithId == null) {
            return null;
        }
        return MAPPER.tableFrom(tableWithId, getIndexes(tableWithId, tableWithId.getTableId(), tenantId));
    }

    @Override
    public Table getTableDef(long schemaId, String tableName) {
        return getTableDef(schemaId, tableName, tenantId);
    }

    @Override
    public Table getTableDef(long schemaId, String tableName, long tenantId) {
        TableDefinitionWithId tableWithId = (TableDefinitionWithId) getTable(schemaId, tableName, tenantId);
        if (tableWithId == null) {
            return null;
        }
        return MAPPER.tableFrom(tableWithId, getIndexes(tableWithId, tableWithId.getTableId(), tenantId));
    }

    @Override
    public IndexTable getIndexDef(long tableId, long indexId) {
        LogUtils.warn(log, "getAllTable by tableId:{}, indexId:{}", tableId, indexId);
        TableDefinitionWithId tableWithId = (TableDefinitionWithId) getTable(tableId);
        if (tableWithId == null) {
            return null;
        }
        Table table =  MAPPER.tableFrom(tableWithId, getIndexes(tableWithId, tableWithId.getTableId(), tenantId));
        return table.getIndexes()
            .stream().filter(indexTable -> indexTable.getTableId().seq == indexId)
            .findFirst().orElse(null);
    }

    @Override
    public IndexTable getIndexDef(long schemaId, long tableId, long indexId) {
        TableDefinitionWithId tableWithId = (TableDefinitionWithId) getTable(schemaId, tableId);
        if (tableWithId == null) {
            return null;
        }
        Table table =  MAPPER.tableFrom(tableWithId, getIndexes(tableWithId, tableWithId.getTableId(), tenantId));
        return table.getIndexes()
            .stream().filter(indexTable -> indexTable.getTableId().seq == indexId)
            .findFirst().orElse(null);
    }

    @Override
    public List<Object> listTable(long schemaId) {
        byte[] tenantKey = tenantKey(tenantId);
        return listTable(schemaId, tenantKey);
    }

    public List<Object> listTable(long schemaId, long tenantId) {
        byte[] tenantKey = tenantKey(tenantId);
        return listTable(schemaId, tenantKey);
    }

    @NonNull
    private List<Object> listTable(long schemaId, byte[] tenantKey) {
        byte[] schemaKey = schemaKey(schemaId);
        if (!checkDBExists(tenantKey, schemaKey)) {
            throw new RuntimeException("schema is null");
        }

        byte[] dataPrefix = CodecKvUtil.hashDataKeyPrefix(schemaKey);
        byte[] end = CodecKvUtil.hashDataKeyPrefixUpperBound(dataPrefix);
        List<byte[]> valueList = txn.mRange(dataPrefix, end);
        if (!valueList.isEmpty()) {
            return valueList.stream().map(val -> getObjFromBytes(val, TableDefinitionWithId.class))
                .map(object -> (TableDefinitionWithId) object)
                .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    @Override
    public List<Object> listTable(String schemaName) {
        byte[] tenantKey = tenantKey(tenantId);
        SchemaInfo schemaInfo = getSchema(schemaName);
        byte[] schemaKey = schemaKey(schemaInfo.getSchemaId());
        if (!checkDBExists(tenantKey, schemaKey)) {
            throw new RuntimeException("schema is null");
        }

        byte[] dataPrefix = CodecKvUtil.hashDataKeyPrefix(schemaKey);
        byte[] end = CodecKvUtil.hashDataKeyPrefixUpperBound(dataPrefix);
        List<byte[]> valueList = txn.mRange(dataPrefix, end);
        if (!valueList.isEmpty()) {
            return valueList.stream().map(val -> getObjFromBytes(val, TableDefinitionWithId.class))
                .map(object -> (TableDefinitionWithId)object)
                .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    @Override
    public List<Object> listIndex(long schemaId, long tableId) {
        return listIndex(schemaId, tableId, tenantId);
    }

    @Override
    public List<Object> listIndex(long schemaId, long tableId, long tenantId) {
        List<byte[]> valueList = getListIndex(schemaId, tableId, tenantId);
        if (!valueList.isEmpty()) {
            return valueList.stream()
                .map(val -> getObjFromBytes(val, TableDefinitionWithId.class))
                .map(objWithId -> (TableDefinitionWithId)objWithId)
                .filter(indexTable -> indexTable.getTableDefinition().isVisible())
                .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    public List<byte[]> getListIndex(long schemaId, long tableId, long tenantId) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        if (!checkDBExists(tenantKey, schemaKey)) {
            throw new RuntimeException("schema is null");
        }
        byte[] tableKey = tableKey(tableId);
        byte[] dataPrefix = CodecKvUtil.hashDataKeyPrefix(tableKey);
        byte[] end = CodecKvUtil.hashDataKeyPrefixUpperBound(dataPrefix);
        return txn.mRange(dataPrefix, end);
    }

    @Override
    public List<Object> allIndex(long schemaId, long tableId) {
        List<byte[]> valueList = getListIndex(schemaId, tableId, tenantId);
        if (!valueList.isEmpty()) {
            return valueList.stream()
                .map(val -> getObjFromBytes(val, TableDefinitionWithId.class))
                .map(objWithId -> (TableDefinitionWithId)objWithId)
                .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    @Override
    public boolean hasIndex(long schemaId, long tableId, String indexName) {
        List<byte[]> valueList = getListIndex(schemaId, tableId, tenantId);
        if (!valueList.isEmpty()) {
            return valueList.stream()
                .map(val -> getObjFromBytes(val, TableDefinitionWithId.class))
                .map(objWithId -> (TableDefinitionWithId)objWithId)
                .anyMatch(indexWithId -> {
                    String ixName = indexWithId.getTableDefinition().getName();
                    return ixName.endsWith(indexName) || ixName.endsWith(indexName.toUpperCase());
                });
        }
        return false;
    }

    @Override
    public Object getIndex(long tableId, long indexId) {
        byte[] tableKey = tableKey(tableId);
        byte[] indexKey = indexKey(indexId);
        byte[] val = txn.hGet(tableKey, indexKey);
        if (val == null) {
            return null;
        }
        return getObjFromBytes(val, TableDefinitionWithId.class);
    }

    @Override
    public void dropTenant(long tenantId) {
        byte[] tenantKey = tenantKey(tenantId);
        txn.hDel(mTenants, tenantKey);
        MetaService.ROOT.deleteTenant(tenantId);
    }

    @Override
    public void dropSchema(long schemaId) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        txn.hDel(tenantKey, schemaKey);
    }

    @Override
    public void dropTable(long schemaId, long tableId) {
        byte[] schemaKey = schemaKey(schemaId);
        byte[] tableKey = tableKey(tableId);
        txn.hDel(schemaKey, tableKey);
    }

    @Override
    public void dropIndex(long tableId, long indexId) {
        byte[] tableKey = tableKey(tableId);
        byte[] indexKey = indexKey(indexId);
        txn.hDel(tableKey, indexKey);
    }

    @Override
    public long genSchemaId() {
        return genId(IdEpochType.ID_NEXT_SCHEMA);
    }

    @Override
    public List<Object> scanRegions(byte[] startKey, byte[] endKey) {
        long startTs = io.dingodb.tso.TsoService.getDefault().tso();
        ScanRegionsRequest request = ScanRegionsRequest.builder()
            .key(startKey)
            .rangeEnd(endKey)
            .limit(0)
            .build();
        CoordinatorService coordinatorService = Services.coordinatorService(coordinators);
        ScanRegionsResponse response = coordinatorService.scanRegions(startTs, request);
        if (response.getRegions() == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(response.getRegions());
    }

    @Override
    public int getStoreReplica() {
        return MetaStoreKv.getActReplica();
    }

    @Override
    public int getIndexReplica() {
        CoordinatorService coordinatorService = Services.coordinatorService(coordinators);
        GetStoreMapRequest storeMapRequest = GetStoreMapRequest.builder().epoch(0).build();
        GetStoreMapResponse response = coordinatorService.getStoreMap(
            System.identityHashCode(storeMapRequest), storeMapRequest
        );
        if (response.getStoremap() == null) {
            return 3;
        }
        long storeCount = response.getStoremap().getStores()
            .stream()
            .filter(store -> store.getStoreType() == StoreType.NODE_TYPE_INDEX
                && store.getState() == StoreState.STORE_NORMAL)
            .count();
        return (int) storeCount;
    }

    @Override
    public int getDocumentReplica() {
        CoordinatorService coordinatorService = Services.coordinatorService(coordinators);
        GetStoreMapRequest storeMapRequest = GetStoreMapRequest.builder().epoch(0).build();
        GetStoreMapResponse response = coordinatorService.getStoreMap(
            System.identityHashCode(storeMapRequest), storeMapRequest
        );
        if (response.getStoremap() == null) {
            return 3;
        }
        long storeCount = response.getStoremap().getStores()
            .stream()
            .filter(store -> store.getStoreType() == StoreType.NODE_TYPE_DOCUMENT
                && store.getState() == StoreState.STORE_NORMAL)
            .count();
        return (int) storeCount;
    }

    @Override
    public long getSchemaVersionWithNonEmptyDiff() {
        long version = this.getSchemaVer();
        if (version == 100) {
            return version;
        }
        SchemaDiff schemaDiff;
        try {
            schemaDiff = this.getSchemaDiff(version);
        } catch (Exception e) {
            log.error("[ddl-error] get schema ver diff error,version:" + version, e);
            return 0;
        }
        if (schemaDiff == null && version > 0) {
            version --;
        }
        return version;
    }

    @Override
    public Map<String, Table> listTableDef(long schemaId) {
        List<Object> tableObjList = listTable(schemaId);
        // check duplicate key TableName
        List<TableDefinitionWithId> duplicateTableList = tableObjList.stream()
            .map(obj -> (TableDefinitionWithId) obj)
            .collect(Collectors.groupingBy(e -> e.getTableDefinition().getName()))
            .values().stream()
            .filter(tableDefinitionWithIds -> tableDefinitionWithIds.size() > 1)
            .map(tableDefinitionWithIds -> tableDefinitionWithIds.get(0))
            .collect(Collectors.toList());
        if (!duplicateTableList.isEmpty()) {
            LogUtils.error(log, "duplicate key Table");
            try {
                duplicateTableList.forEach(e -> {
                    io.dingodb.meta.MetaService.root()
                        .dropTable(tenantId, schemaId, e.getTableDefinition().getName(), -1);
                });
            } catch (Exception e) {
                LogUtils.error(log, e.getMessage(), e);
            }
        }
        return tableObjList.stream()
            .map(obj -> (TableDefinitionWithId) obj)
            .map(tableWithId -> MAPPER.tableFrom(tableWithId,
                getIndexes(tableWithId, tableWithId.getTableId(), tenantId)))
            .collect(Collectors.toConcurrentMap(t -> t.name, t -> t));
    }

    @Override
    public Map<String, Table> listTableDef(long schemaId, long tenantId) {
        List<Object> objList = listTable(schemaId, tenantId);
        return objList.stream()
            .map(obj -> (TableDefinitionWithId) obj)
            .map(tableWithId -> MAPPER.tableFrom(tableWithId,
                getIndexes(tableWithId, tableWithId.getTableId(), tenantId)))
            .collect(Collectors.toConcurrentMap(t -> t.name, t -> t));
    }

    @Override
    public void putKvToCoordinator(String key, String val) {
        PutRequest putRequest = putRequest(key, val);
        putKvToCoordinator(putRequest, 3);
    }

    public void putKvToCoordinator(PutRequest putRequest, int retry) {
        try {
            versionService.kvPut(System.identityHashCode(putRequest), putRequest);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            if (retry-- > 0) {
                resetVerService();
                putKvToCoordinator(putRequest, retry);
            }
        }
    }

    @Override
    public void delKvFromCoordinator(String key, String keyEnd) {
        DeleteRangeRequest deleteRequest = DeleteRangeRequest.builder()
            .key(key.getBytes()).rangeEnd(keyEnd.getBytes()).build();
        DeleteRangeResponse response = versionService.kvDeleteRange(
            System.identityHashCode(deleteRequest), deleteRequest
        );
        if (response.getDeleted() < 1) {
            LogUtils.error(log, "del kv failed,key:{}, keyEnd:{}", key, keyEnd);
        }
    }

    @Override
    public List<io.dingodb.common.store.KeyValue> getByKey(String key, String keyEnd) {
        RangeRequest rangeRequest = RangeRequest.builder()
            .key(key.getBytes())
            .rangeEnd(keyEnd.getBytes())
            .build();
        RangeResponse response = versionService.kvRange(System.identityHashCode(rangeRequest), rangeRequest);
        if (response.getKvs() == null) {
            return new ArrayList<>();
        }
        return response.getKvs()
            .stream().filter(Objects::nonNull).map(Kv::getKv)
            .map(kv -> new io.dingodb.common.store.KeyValue(kv.getKey(), kv.getValue()))
            .collect(Collectors.toList());
    }

    @Override
    public synchronized Long genSchemaVersion(long step) {
        byte[] ek = genSchemaVerKey;
        long id = 0L;

        byte[] val = txn.ddlGet(ek);
        if (val != null) {
            id = Long.parseLong(new String(val));
        }

        id += step;
        byte[] valBytes = String.valueOf(id).getBytes();

        txn.ddlPut(ek, valBytes);

        return id;
    }

    @Override
    public void setSchemaDiff(SchemaDiff schemaDiff) {
        byte[] data = getBytesFromObj(schemaDiff);
        byte[] key = schemaDiffKey(schemaDiff.getVersion());
        this.txn.ddlPut(key, data);
    }

    @Override
    public void delSchemaDiff(long ver) {
        byte[] key = schemaDiffKey(ver);
        this.txn.ddlDel(key);
    }

    @Override
    public void updateTable(long schemaId, Object table) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        if (!checkDBExists(tenantKey, schemaKey)) {
            throw new RuntimeException("schema is null");
        }
        TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) table;
        byte[] tableKey = tableKey(tableDefinitionWithId.getTableId().getEntityId());
        byte[] val = getBytesFromObj(table);
        this.txn.hPut(schemaKey, tableKey, val);
    }

    @Override
    public void updateReplicaTable(long schemaId, long tableId, Object table) {
        TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) table;
        byte[] tableKey = tableKey(tableId);

        byte[] val = getBytesFromObj(table);
        byte[] replicaKey = indexKey(tableDefinitionWithId.getTableId().getEntityId());
        this.txn.hPut(tableKey, replicaKey, val);
    }

    @Override
    public void updateIndex(long tableId, Object index) {
        byte[] tableKey = tableKey(tableId);
        TableDefinitionWithId indexInfo = (TableDefinitionWithId) index;
        byte[] indexKey = indexKey(indexInfo.getTableId().getEntityId());
        byte[] val = getBytesFromObj(index);
        this.txn.hPut(tableKey, indexKey, val);
    }

    @Override
    public DdlJob getHistoryDDLJob(long jobId) {
        byte[] val = this.txn.ddlHGet(mHistoryJobPrefixKeys, jobIdKey(jobId));
        if (val == null) {
            return null;
        }
        return (DdlJob) getObjFromBytes(val, DdlJob.class);
    }

    @Override
    public void addHistoryDDLJob(DdlJob job, boolean updateRawArgs) {
        byte[] data = job.encode(updateRawArgs);
        this.txn.ddlHPut(mHistoryJobPrefixKeys, jobIdKey(job.getId()), data);
    }

    @Override
    public void delHistoryDDLJob(long jobId) {
        this.txn.ddlHDel(mHistoryJobPrefixKeys, jobIdKey(jobId));
    }

    @Override
    public void prepareDone() {
        long ver = this.getSchemaVer();
        LogUtils.info(log, "prepareDone, current ver:" + ver);
        genSchemaVersion(100);
        ver = getSchemaVer();

        versionService.kvPut(putRequest(globalSchemaVer, "100"));
        LogUtils.info(log, "prepare done current ver:" + ver);
    }

    @Override
    public void prepareStart() {
        long ver = this.getSchemaVer();
        LogUtils.info(log, "prepareStart, current ver:" + ver);
        if (ver == 0) {
            versionService.kvPut(putRequest(globalSchemaVer, "1"));
            LogUtils.info(log, "prepare start");
        } else {
            LogUtils.error(log, "prepare already started");
        }
    }

    @Override
    public boolean prepare() {
        long version = getSchemaVer();
        LogUtils.info(log, "validate prepare done, newest schema ver:" + version);
        return version >= 100;
    }

    @Override
    public boolean prepareStarted() {
        long version = getSchemaVer();
        LogUtils.info(log, "validate prepare start, newest schema ver:" + version);
        return version >= 1;
    }

    @Override
    public synchronized List<Long> genGlobalIDs(int number) {
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
        CreateIdsRequest request = CreateIdsRequest.builder()
            .idEpochType(IdEpochType.ID_DDL_JOB).count(number)
            .build();
        long ts = System.identityHashCode(request);
        return coordinatorService.createIds(
            ts,
            request
        ).getIds();
    }

    @Override
    public InfoSchema getInfoSchemaByTenantId(long tenantId) {
        long neededSchemaVersion = this.getSchemaVerByTenant(tenantId);
        List<SchemaInfo> schemaInfoList = listSchema(tenantId);
        InfoSchemaBuilder builder = new InfoSchemaBuilder();
        builder.initWithSchemaInfosByTenant(schemaInfoList, neededSchemaVersion, this, tenantId);
        return builder.build();
    }

    private long genId(IdEpochType idEpochType) {
        long startTs = TsoService.INSTANCE.tso();
        CoordinatorService coordinatorService = Services.coordinatorService(coordinators);
        CreateIdsRequest request = CreateIdsRequest.builder().count(1).idEpochType(idEpochType).build();
        CreateIdsResponse response = coordinatorService.createIds(startTs, request);
        if (!response.getIds().isEmpty()) {
            return response.getIds().get(0);
        }
        return 0;
    }

    private static PutRequest putRequest(String resourceKey, String value) {
        return PutRequest.builder()
            .lease(0L)
            .ignoreValue(value == null || value.isEmpty())
            .keyValue(KeyValue.builder()
                .key(resourceKey.getBytes(UTF_8))
                .value(value == null ? null : value.getBytes(UTF_8))
                .build())
            .needPrevKv(true)
            .build();
    }

    public SchemaDiff getSchemaDiff(long schemaVersion) {
        byte[] key = schemaDiffKey(schemaVersion);
        byte[] val = this.txn.ddlGet(key);
        if (val == null) {
            return null;
        }
        Object object = getObjFromBytes(val, SchemaDiff.class);
        return (SchemaDiff) object;
    }

    private List<TableDefinitionWithId> getIndexes(
        TableDefinitionWithId tableWithId,
        DingoCommonId tableId,
        long tenantId
    ) {
        try {
            if (tableWithId.getTableId().getEntityType() == EntityType.ENTITY_TYPE_INDEX) {
                return new ArrayList<>();
            }
            List<Object> indexList = this
                .listIndex(tableId.getParentEntityId(), tableId.getEntityId(), tenantId);
            return indexList.stream()
                .map(object -> (TableDefinitionWithId) object)
                .filter(indexTable -> indexTable.getTableDefinition().isVisible())
                .peek(indexWithId -> {
                    String name1 = indexWithId.getTableDefinition().getName();
                    String[] split = name1.split("\\.");
                    if (split.length > 1) {
                        name1 = split[split.length - 1];
                    }
                    indexWithId.getTableDefinition().setName(name1);
                })
                .collect(Collectors.toList());
        } catch (Exception e) {
            if (tableWithId != null) {
                LogUtils.error(log, "getIndexes tableWithId:" + tableWithId);
            } else {
                LogUtils.error(log, "getIndexes tableWithId is null");
            }
            throw e;
        }
    }

    public long getSchemaVer() {
        long id = 0L;
        byte[] valBytes = txn.ddlGet(genSchemaVerKey);
        if (valBytes != null) {
            id = Long.parseLong(new String(valBytes));
        }
        return id;
    }

    public long getSchemaVerByTenant(long tenantId) {
        byte[] key = CodecKvUtil.encodeStringDataKey(schemaVerKeyByTenant(tenantId));
        RangeRequest rangeRequest = RangeRequest.builder()
            .key(key)
            .build();
        RangeResponse response = versionService
            .kvRange(System.identityHashCode(rangeRequest), rangeRequest);

        long id = 0L;
        if (response.getKvs() != null && !response.getKvs().isEmpty()) {
            byte[] val = response.getKvs().get(0).getKv().getValue();
            id = Long.parseLong(new String(val));
        }
        return id;
    }

    public void resetVerService() {
        Services.invalidateVersionService(coordinators);
        this.versionService = Services.versionService(coordinators);
    }

}
