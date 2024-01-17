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

import com.google.auto.service.AutoService;
import io.dingodb.meta.InfoSchemaServiceProvider;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.VersionService;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.sdk.service.entity.version.PutRequest;
import io.dingodb.sdk.service.entity.version.RangeRequest;
import io.dingodb.sdk.service.entity.version.RangeResponse;
import io.dingodb.store.proxy.Configuration;
import lombok.extern.slf4j.Slf4j;

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
    private final VersionService versionService;
    public static final InfoSchemaService ROOT = new InfoSchemaService(Configuration.coordinators());

    @AutoService(InfoSchemaServiceProvider.class)
    public static class Provider implements InfoSchemaServiceProvider {
        @Override
        public io.dingodb.meta.InfoSchemaService root() {
            return ROOT;
        }
    }

    private InfoSchemaService(String coordinators) {
        this.versionService = Services.versionService(Services.parse(coordinators));
    }

    @Override
    public Map<String, String> getGlobalVariables() {
        RangeResponse response = versionService.kvRange(rangeRequest());
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
            log.info(e.getMessage(), e);
        }
        return variableMap;
    }


    @Override
    public void putGlobalVariable(String key, Object val) {
        String resourceKey = GLOBAL_VAR_PREFIX_BEGIN + key;
        versionService.kvPut(putRequest(resourceKey, val.toString()));
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
