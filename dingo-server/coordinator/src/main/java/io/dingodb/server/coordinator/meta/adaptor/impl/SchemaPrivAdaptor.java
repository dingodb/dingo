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

package io.dingodb.server.coordinator.meta.adaptor.impl;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.store.MetaStore;
import io.dingodb.server.protocol.meta.SchemaPriv;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.PRIVILEGE_IDENTIFIER;
import static io.dingodb.server.protocol.CommonIdConstant.ROOT_DOMAIN;

@Slf4j
public class SchemaPrivAdaptor extends BaseAdaptor<SchemaPriv> {
    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.data, PRIVILEGE_IDENTIFIER.schemaPrivilege);

    protected final Map<String, SchemaPriv> schemaPrivMap;

    public SchemaPrivAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(SchemaPriv.class, this);
        schemaPrivMap = new ConcurrentHashMap<>();

        metaMap.forEach((k, v) -> {
            schemaPrivMap.put(v.getKey(), v);
        } );
        log.info("init schemaPrivMap:" + schemaPrivMap);
    }

    @Override
    protected CommonId newId(SchemaPriv meta) {
        return new CommonId(
            META_ID.type(),
            META_ID.identifier(), ROOT_DOMAIN,
            metaStore.generateSeq(CommonId.prefix(META_ID.type(), META_ID.identifier()).encode())
        );
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    public Map<String, List<SchemaPrivDefinition>> getAllDefinition() {
        return getAll().stream().map(this::metaToDefinition)
            .collect(Collectors.groupingBy(SchemaPrivDefinition::getUser));
    }

    private SchemaPrivDefinition metaToDefinition(SchemaPriv tablePriv) {
        return new SchemaPrivDefinition();
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator implements BaseAdaptor.Creator<SchemaPriv, SchemaPrivAdaptor> {
        @Override
        public SchemaPrivAdaptor create(MetaStore metaStore) {
            return new SchemaPrivAdaptor(metaStore);
        }
    }

    /**
     * getSchemaPrivilegesWithCurrentHost.
     * @param user userName
     * @param host host
     * @return schema privilege map.
     */
    public Map<CommonId, SchemaPriv> getSchemaPrivilegesWithCurrentHost(String user, String host) {
        return schemaPrivMap.entrySet().stream()
            .filter(k ->
                k.getKey().startsWith(user + "#" + host + "#")
            )
            .map(Map.Entry :: getValue)
            .collect(Collectors.toMap(SchemaPriv::getSchema, Function.identity()));
    }

    public CommonId create(SchemaPrivDefinition schemaPrivDefinition) {
        String schemaPrivKey = schemaPrivDefinition.getKey();
        log.info("schema privilege map:" + schemaPrivMap);
        if (schemaPrivMap.containsKey(schemaPrivKey)) {
            return schemaPrivMap.get(schemaPrivKey).getId();
        } else {
            SchemaPriv schemaPriv = definitionToMeta(schemaPrivDefinition);
            schemaPriv.setId(newId(schemaPriv));
            doSave(schemaPriv);
            return schemaPriv.getId();
        }
    }

    @Override
    protected void doSave(SchemaPriv schemaPriv) {
        super.doSave(schemaPriv);
        schemaPrivMap.putIfAbsent(schemaPriv.getKey(), schemaPriv);
    }

    private SchemaPriv definitionToMeta(SchemaPrivDefinition definition) {
        return SchemaPriv.builder().user(definition.getUser())
            .host(definition.getHost())
            .schema(definition.getSchema())
            .build();
    }

    @Override
    protected void doDelete(SchemaPriv meta) {
        super.doDelete(meta);
    }

    public CommonId delete(SchemaPrivDefinition definition) {
        SchemaPriv schemaPriv = schemaPrivMap.remove(definition.getKey());
        if (schemaPriv != null) {
            doDelete(schemaPriv);
            log.info("Delete schema privilege, schema privilege map = {}", schemaPrivMap);
            return schemaPriv.getId();
        } else {
            log.error("Delete schema privilege is null.");
        }
        return null;
    }
}
