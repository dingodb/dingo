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
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.VersionService;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.version.DeleteRangeRequest;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.sdk.service.entity.version.PutRequest;
import io.dingodb.sdk.service.entity.version.RangeRequest;
import io.dingodb.sdk.service.entity.version.RangeResponse;
import io.dingodb.store.proxy.Configuration;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.common.mysql.InformationSchemaConstant.GLOBAL_VAR_PREFIX_BEGIN;
import static io.dingodb.common.mysql.InformationSchemaConstant.GLOBAL_VAR_PREFIX_END;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class InfoSchemaService implements io.dingodb.meta.InfoSchemaService {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final VersionService versionService;
    public static final InfoSchemaService ROOT = new InfoSchemaService(Configuration.coordinators());

    @AutoService(InfoSchemaServiceProvider.class)
    public static class Provider implements InfoSchemaServiceProvider {
        @Override
        public io.dingodb.meta.InfoSchemaService root() {
            return ROOT;
        }
    }

    public InfoSchemaService(String coordinators) {
        this.versionService = Services.versionService(Services.parse(coordinators));
    }

    @Override
    public Map<String, String> getGlobalVariables() {
        RangeRequest rangeRequest = rangeRequest();
        RangeResponse response = versionService.kvRange((long)System.identityHashCode(rangeRequest), rangeRequest);
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

    @Override
    public boolean checkTableExists(byte[] schemaKey, byte[] tableKey) {
        byte[] value = hGet(schemaKey, tableKey);
        return value != null;
    }

    @Override
    public void createTableOrView(long tenantId, long schemaId, long tableId, Object table) {
        byte[] tenantKey = tenantKey(tenantId);
        byte[] schemaKey = schemaKey(schemaId);
        if (!checkDBExists(tenantKey, schemaKey)) {
            throw new RuntimeException("schema is null");
        }
        byte[] tableKey = tableKey(tableId);
        if (checkTableExists(schemaKey, tableKey)) {
            throw new RuntimeException("table has exists");
        }
        byte[] val = getBytesFromObj(table);
        hSet(schemaKey, tableKey, val);
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
    public void createSchema(long tenantId, long schemaId, Object schema) {
        byte[] tenantKey = tenantKey(tenantId);
        if (!checkTenantExists(tenantKey)) {
            throw new RuntimeException("tenant is null");
        }
        byte[] schemaKey = schemaKey(schemaId);
        if (checkDBExists(tenantKey, schemaKey)) {
            throw new RuntimeException("table has exists");
        }
        byte[] val = getBytesFromObj(schema);
        hSet(tenantKey, schemaKey, val);
    }

    @Override
    public void createTenant(long tenantId, Object tenant) {
        byte[] tenantKey = tenantKey(tenantId);
        if (checkTenantExists(tenantKey)) {
            throw new RuntimeException("tenant is exists");
        }
        byte[] val = getBytesFromObj(tenant);
        hSet(mTenants, tenantKey, val);
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
        RangeRequest rangeRequest = RangeRequest.builder()
            .key(dataPrefix)
            .rangeEnd(end)
            .build();
        RangeResponse response = versionService.kvRange(rangeRequest);
        if (response.getKvs() != null) {
            return response.getKvs().stream().map(kv -> kv.getKv().getValue())
                .map(v -> getObjFromBytes(v, Tenant.class))
                .collect(Collectors.toList());
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
    public List<Object> listSchema(long tenantId) {
        byte[] tenantKey = tenantKey(tenantId);

        byte[] dataPrefix = CodecKvUtil.hashDataKeyPrefix(tenantKey);
        byte[] end = CodecKvUtil.hashDataKeyPrefixUpperBound(dataPrefix);
        RangeRequest rangeRequest = RangeRequest.builder()
            .key(dataPrefix)
            .rangeEnd(end)
            .build();
        RangeResponse response = versionService.kvRange(rangeRequest);
        if (response.getKvs() != null) {
            return response.getKvs().stream()
                .map(kv -> kv.getKv().getValue())
                .map(val -> getObjFromBytes(val, SchemaInfo.class))
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
        if (checkTableExists(schemaKey, tableKey)) {
            throw new RuntimeException("table has exists");
        }
        byte[] val = hGet(schemaKey, tableKey);
        // todo transform
        return val;
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
        RangeRequest rangeRequest = RangeRequest.builder()
            .key(dataPrefix)
            .rangeEnd(end)
            .build();
        RangeResponse response = versionService.kvRange(rangeRequest);
        if (response.getKvs() != null) {
            return response.getKvs().stream().map(kv -> kv.getKv().getValue()).collect(Collectors.toList());
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


    public byte[] hGet(byte[] key, byte[] field) {
        byte[] dataKey = CodecKvUtil.encodeHashDataKey(key, field);
        RangeRequest rangeRequest = RangeRequest.builder()
            .key(dataKey)
            .build();
        RangeResponse response = versionService.kvRange(System.identityHashCode(rangeRequest), rangeRequest);
        if (response.getKvs() != null && !response.getKvs().isEmpty()) {
            return response.getKvs().get(0).getKv().getValue();
        }
        return null;
    }

    public void hSet(byte[] key, byte[] field, byte[] value) {
        byte[] dataKey = CodecKvUtil.encodeHashDataKey(key, field);
        PutRequest request = putRequest(dataKey, value);
        versionService.kvPut(System.identityHashCode(request), request);
    }

    public void hDel(byte[] key, byte[] field) {
        byte[] dataKey = CodecKvUtil.encodeHashDataKey(key, field);
        DeleteRangeRequest request = DeleteRangeRequest.builder().key(dataKey).build();
        versionService.kvDeleteRange(System.identityHashCode(request), request);
    }

    public void hDel(byte[] key, List<byte[]> fields) {
        for (byte[] field : fields) {
            byte[] dataKey = CodecKvUtil.encodeHashDataKey(key, field);
            DeleteRangeRequest request = DeleteRangeRequest.builder().rangeEnd(dataKey).build();
            versionService.kvDeleteRange(System.identityHashCode(request), request);
        }
    }

    private static PutRequest putRequest(byte[] key, byte[] value) {
        return PutRequest.builder()
            .lease(0L)
            .ignoreValue(value == null)
            .keyValue(KeyValue.builder()
                .key(key)
                .value(value)
                .build())
            .needPrevKv(true)
            .build();
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
