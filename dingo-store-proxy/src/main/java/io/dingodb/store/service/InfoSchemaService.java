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
import io.dingodb.common.codec.CodecKvUtil;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.meta.Tenant;
import io.dingodb.meta.InfoSchemaServiceProvider;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.VersionService;
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
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.sdk.service.entity.version.PutRequest;
import io.dingodb.sdk.service.entity.version.RangeRequest;
import io.dingodb.sdk.service.entity.version.RangeResponse;
import io.dingodb.store.proxy.Configuration;

import io.dingodb.store.proxy.service.TsoService;
import lombok.extern.slf4j.Slf4j;

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
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class InfoSchemaService implements io.dingodb.meta.InfoSchemaService {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final VersionService versionService;
    Set<Location> coordinators;
    public static final InfoSchemaService ROOT = new InfoSchemaService(Configuration.coordinators());

    @AutoService(InfoSchemaServiceProvider.class)
    public static class Provider implements InfoSchemaServiceProvider {
        @Override
        public io.dingodb.meta.InfoSchemaService root() {
            return ROOT;
        }
    }

    public InfoSchemaService(String coordinators) {
        this.coordinators = Services.parse(coordinators);
        this.versionService = Services.versionService(this.coordinators);
    }

    @Override
    public Map<String, String> getGlobalVariables() {
        RangeRequest rangeRequest = rangeRequest();
        RangeResponse response = versionService.kvRange(System.identityHashCode(rangeRequest), rangeRequest);
        List<KeyValue> res = response.getKvs()
            .stream().filter(Objects::nonNull).map(Kv::getKv)
            .collect(Collectors.toList());
        Map<String, String> variableMap = new LinkedHashMap<>();
        try {
            for (KeyValue keyValue : res) {
                String value = "";
                if (keyValue.getValue() != null) {
                    value = new String(keyValue.getValue(), UTF_8);
                }
                String fullKey = new String(keyValue.getKey(), UTF_8);
                variableMap.put(
                    fullKey.substring(GLOBAL_VAR_PREFIX_BEGIN.length()),
                    value
                );
            }
            return variableMap;
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        }
        return variableMap;
    }


    @Override
    public void putGlobalVariable(String key, Object val) {
        String resourceKey = GLOBAL_VAR_PREFIX_BEGIN + key;
        versionService.kvPut(putRequest(resourceKey, val.toString()));
    }

    @Override
    public boolean checkTenantExists(byte[] tenantKey) {
        byte[] val = hGet(mTenants, tenantKey);
        return val != null;
    }

    @Override
    public boolean checkDBExists(byte[] tenantKey, byte[] schemaKey) {
        byte[] val = hGet(tenantKey, schemaKey);
        return val != null;
    }

    public boolean checkSchemaNameExists(long tenantId, String schemaName){
        List<SchemaInfo> schemaInfoList = listSchema(tenantId);
        return schemaInfoList.stream()
            .anyMatch(schemaInfo -> schemaInfo.getName().equalsIgnoreCase(schemaName));
    }

    @Override
    public boolean checkTableExists(byte[] schemaKey, byte[] tableKey) {
        byte[] value = hGet(schemaKey, tableKey);
        return value != null;
    }

    public boolean checkTableNameExists(long tenantId, long schemaId, String tableName) {
        List<Object> tableDefinitionWithIds = listTable(tenantId, schemaId);
        return tableDefinitionWithIds.stream().map(object -> (TableDefinitionWithId)object)
            .anyMatch(tableDefinitionWithId ->
                tableDefinitionWithId.getTableDefinition().getName().equalsIgnoreCase(tableName)
                );
    }

    @Override
    public void createTableOrView(long tenantId, long schemaId, long tableId, Object table) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        if (!checkDBExists(tenantKey, schemaKey)) {
            throw new RuntimeException("schema is null");
        }
        byte[] tableKey = tableKey(tableId);
        TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) table;
        if (checkTableNameExists(tenantId, schemaId, tableDefinitionWithId.getTableDefinition().getName())) {
            throw new RuntimeException("table has exists");
        }
        byte[] val = getBytesFromObj(table);
        hSet(schemaKey, tableKey, val);
    }

    @Override
    public void createIndex(long tenantId, long schemaId, long tableId, Object index) {
        byte[] tableKey = tableKey(tableId);
        TableDefinitionWithId indexWithId = (TableDefinitionWithId) index;

        byte[] indexKey = indexKey(indexWithId.getTableId().getEntityId());
        byte[] val = getBytesFromObj(index);
        hSet(tableKey, indexKey, val);
    }

    private byte[] getBytesFromObj(Object table) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            objectMapper.writeValue(outputStream, table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return outputStream.toByteArray();
    }

    private Object getObjFromBytes(byte[] val, Class type) {
        try {
            return objectMapper.readValue(val, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createSchema(long tenantId, long schemaId, SchemaInfo schema) {
        byte[] tenantKey = tenantKey(tenantId);
        if (!checkTenantExists(tenantKey)) {
            throw new RuntimeException("tenant is null");
        }
        byte[] schemaKey = schemaKey(schemaId);
        if (checkSchemaNameExists(tenantId, schema.getName())) {
            return;
        }
        byte[] val = getBytesFromObj(schema);
        hSet(tenantKey, schemaKey, val);
    }

    @Override
    public boolean createTenant(long tenantId, Object tenant) {
        byte[] tenantKey = tenantKey(tenantId);
        if (checkTenantExists(tenantKey)) {
            return false;
        }
        byte[] val = getBytesFromObj(tenant);
        hSet(mTenants, tenantKey, val);
        return true;
    }

    @Override
    public Object getTenant(long tenantId) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] val = hGet(mTenants, tenantKey);
        return getObjFromBytes(val, Tenant.class);
    }

    @Override
    public List<Object> listTenant() {
        byte[] dataPrefix = CodecKvUtil.hashDataKeyPrefix(mTenants);
        byte[] end = CodecKvUtil.hashDataKeyPrefixUpperBound(dataPrefix);
        List<byte[]> valueList = MetaStoreKvTxn.getInstance().mRange(dataPrefix, end);
        if (!valueList.isEmpty()) {
            return valueList.stream().map(val -> getObjFromBytes(val, Tenant.class)).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    @Override
    public Object getSchema(long tenantId, long schemaId) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        byte[] val = hGet(tenantKey, schemaKey);
        return getObjFromBytes(val, SchemaInfo.class);
    }

    @Override
    public SchemaInfo getSchema(long tenantId, String schemaName) {
        List<SchemaInfo> schemaList = listSchema(tenantId);
        return schemaList.stream()
            .map(object -> (SchemaInfo) object)
            .filter(schemaInfo1 -> schemaInfo1.getName().equalsIgnoreCase(schemaName))
            .findFirst().orElse(null);
    }

    @Override
    public List<SchemaInfo> listSchema(long tenantId) {
        byte[] tenantKey = tenantKey(tenantId);

        byte[] dataPrefix = CodecKvUtil.hashDataKeyPrefix(tenantKey);
        byte[] end = CodecKvUtil.hashDataKeyPrefixUpperBound(dataPrefix);
        List<byte[]> valueList = MetaStoreKvTxn.getInstance().mRange(dataPrefix, end);
        if (!valueList.isEmpty()) {
            return valueList
              .stream()
              .map(val -> getObjFromBytes(val, SchemaInfo.class)).map(object -> (SchemaInfo)object)
              .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    @Override
    public Object getTable(long tenantId, long schemaId, long tableId) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        if (!checkDBExists(tenantKey, schemaKey)) {
            throw new RuntimeException("schema is null");
        }
        byte[] tableKey = tableKey(tableId);
        byte[] val = hGet(schemaKey, tableKey);
        if (val == null) {
            return null;
        }
        return getObjFromBytes(val, TableDefinitionWithId.class);
    }

    @Override
    public Object getTable(long tenantId, long schemaId, String tableName) {
        List<Object> tableList = listTable(tenantId, schemaId);
        return tableList.stream().map(object -> (TableDefinitionWithId)object)
            .filter(tableDefinitionWithId -> tableDefinitionWithId.getTableDefinition().getName().equalsIgnoreCase(tableName))
            .findFirst().orElse(null);
    }

    @Override
    public Object getTable(long tenantId, String schemaName, String tableName) {
        SchemaInfo schemaInfo = getSchema(tenantId, schemaName);
        return getTable(tenantId, schemaInfo.getSchemaId(), tableName);
    }

    @Override
    public Object getTable(long tenantId, long tableId) {
        List<SchemaInfo> schemaList = listSchema(tenantId);
        return schemaList.stream()
            .map(schemaInfo -> listTable(tenantId, schemaInfo.getSchemaId()))
            .map(tableList -> tableList.stream().filter(object -> {
                    TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) object;
                    return tableDefinitionWithId.getTableId().getEntityId() == tableId;
                }).findAny().orElse(null))
            .filter(Objects::nonNull)
            .findFirst().orElse(null);
    }

    @Override
    public List<Object> listTable(long tenantId, long schemaId) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        if (!checkDBExists(tenantKey, schemaKey)) {
            throw new RuntimeException("schema is null");
        }

        byte[] dataPrefix = CodecKvUtil.hashDataKeyPrefix(schemaKey);
        byte[] end = CodecKvUtil.hashDataKeyPrefixUpperBound(dataPrefix);
        List<byte[]> valueList = MetaStoreKvTxn.getInstance().mRange(dataPrefix, end);
        if (!valueList.isEmpty()) {
            return valueList.stream().map(val -> getObjFromBytes(val, TableDefinitionWithId.class))
                .map(object -> (TableDefinitionWithId)object)
                .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    @Override
    public List<Object> listIndex(long tenantId, long schemaId, long tableId) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        if (!checkDBExists(tenantKey, schemaKey)) {
            throw new RuntimeException("schema is null");
        }
        byte[] tableKey = tableKey(tableId);
        byte[] dataPrefix = CodecKvUtil.hashDataKeyPrefix(tableKey);
        byte[] end = CodecKvUtil.hashDataKeyPrefixUpperBound(dataPrefix);
        List<byte[]> valueList = MetaStoreKvTxn.getInstance().mRange(dataPrefix, end);
        if (!valueList.isEmpty()) {
            return valueList.stream().map(val -> getObjFromBytes(val, TableDefinitionWithId.class))
                .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    @Override
    public void dropTenant(long tenantId) {
        byte[] tenantKey = tenantKey(tenantId);
        hDel(mTenants, tenantKey);
    }

    @Override
    public void dropSchema(long tenantId, long schemaId) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        hDel(tenantKey, schemaKey);
    }

    @Override
    public void dropTable(long tenantId, long schemaId, long tableId) {
        byte[] schemaKey = schemaKey(schemaId);
        byte[] tableKey = tableKey(tableId);
        hDel(schemaKey, tableKey);
    }

    @Override
    public long genSchemaId() {
        return genId(IdEpochType.ID_NEXT_SCHEMA);
    }

    @Override
    public long genTenantId() {
        return genId(IdEpochType.ID_NEXT_TENANT);
    }

    @Override
    public long genTableId() {
        return genId(IdEpochType.ID_NEXT_TABLE);
    }

    @Override
    public long genIndexId() {
        return genId(IdEpochType.ID_NEXT_INDEX);
    }

    @Override
    public List<Object> scanRegions(byte[] startKey, byte[] endKey) {
        long startTs = io.dingodb.tso.TsoService.getDefault().tso();
        ScanRegionsRequest request = ScanRegionsRequest.builder()
            .key(startKey)
            .rangeEnd(endKey)
            .limit(1)
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
            .filter(store -> store.getStoreType() != StoreType.NODE_TYPE_INDEX
                && store.getState() == StoreState.STORE_NORMAL)
            .count();
        return (int) storeCount;
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


    public static byte[] hGet(byte[] key, byte[] field) {
        byte[] dataKey = CodecKvUtil.encodeHashDataKey(key, field);
        return MetaStoreKvTxn.getInstance().mGet(dataKey);
    }

    public static void hSet(byte[] key, byte[] field, byte[] value) {
        byte[] dataKey = CodecKvUtil.encodeHashDataKey(key, field);
        MetaStoreKvTxn.getInstance().mInsert(dataKey, value);
    }

    public static void hDel(byte[] key, byte[] field) {
        byte[] dataKey = CodecKvUtil.encodeHashDataKey(key, field);
        MetaStoreKvTxn.getInstance().mDel(dataKey);
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

    private static RangeRequest rangeRequest() {
        return RangeRequest.builder()
            .key(GLOBAL_VAR_PREFIX_BEGIN.getBytes(UTF_8))
            .rangeEnd(GLOBAL_VAR_PREFIX_END.getBytes(UTF_8))
            .build();
    }


}
