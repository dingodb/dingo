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

package io.dingodb.server.coordinator.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;
import io.dingodb.net.Message;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.service.ListenService;
import io.dingodb.server.coordinator.config.Configuration;
import io.dingodb.server.coordinator.meta.Constant;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.adaptor.impl.TableAdaptor;
import io.dingodb.server.protocol.MetaListenEvent;
import io.dingodb.server.protocol.meta.Schema;
import io.dingodb.server.protocol.meta.Table;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.meta.MetaService.DINGO_NAME;
import static io.dingodb.meta.MetaService.META_NAME;
import static io.dingodb.server.coordinator.meta.Constant.DINGO_SCHEMA_ID;
import static io.dingodb.server.coordinator.meta.Constant.DINGO_SCHEMA_SEQ;
import static io.dingodb.server.coordinator.meta.Constant.META_SCHEMA;
import static io.dingodb.server.coordinator.meta.Constant.META_SCHEMA_ID;
import static io.dingodb.server.coordinator.meta.Constant.META_SCHEMA_SEQ;
import static io.dingodb.server.coordinator.meta.Constant.ROOT_SCHEMA;
import static io.dingodb.server.coordinator.meta.Constant.ROOT_SCHEMA_ID;
import static io.dingodb.server.coordinator.meta.Constant.ROOT_SCHEMA_SEQ;
import static io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry.getMetaAdaptor;
import static io.dingodb.server.protocol.ListenerTags.MetaListener.SCHEMA;
import static io.dingodb.server.protocol.MetaListenEvent.Event.CREATE_TABLE;
import static io.dingodb.server.protocol.MetaListenEvent.Event.DELETE_TABLE;

public class MetaServiceApi implements io.dingodb.server.api.MetaServiceApi {

    public static final MetaServiceApi INSTANCE;

    static {
        ApiRegistry.getDefault().register(io.dingodb.server.api.MetaServiceApi.class, INSTANCE = new MetaServiceApi());
    }

    private final TableAdaptor adaptor = getMetaAdaptor(Table.class);
    private final Map<CommonId, Consumer<Message>> schemaListener = new ConcurrentHashMap<>();
    private final ListenService listenService = ListenService.getDefault();

    private MetaServiceApi() {
        schemaListener.put(ROOT_SCHEMA_ID, listenService.register(ROOT_SCHEMA_ID, SCHEMA));
        schemaListener.put(META_SCHEMA_ID, listenService.register(META_SCHEMA_ID, SCHEMA));
        schemaListener.put(DINGO_SCHEMA_ID, listenService.register(DINGO_SCHEMA_ID, SCHEMA));

        Collection<Schema> schemas = getMetaAdaptor(Schema.class).getAll();
        for (Schema schema : schemas) {
            schemaListener.put(schema.getId(), listenService.register(schema.getId(), SCHEMA));
        }
    }

    @Override
    public CommonId rootId() {
        return ROOT_SCHEMA_ID;
    }

    @Override
    public CommonId createSubMetaService(CommonId id, String name) {
        if (id.seq == Constant.META_SCHEMA_SEQ) {
            throw new UnsupportedOperationException("Meta schema cannot create sub schema");
        }
        CommonId schemaId = getMetaAdaptor(Schema.class).save(Schema.builder().name(name).parent(id).build());
        schemaListener.put(id, listenService.register(schemaId, SCHEMA));
        return schemaId;
    }

    @Override
    public List<Schema> getSubSchemas(CommonId id) {
        List<Schema> schemas = getMetaAdaptor(Schema.class).getByDomain(id.seq);
        if (id.seq == Constant.ROOT_SCHEMA_SEQ) {
            schemas.add(Constant.DINGO_SCHEMA);
            schemas.add(META_SCHEMA);
        }
        return schemas;
    }

    @Override
    public Schema getSubSchema(CommonId id, String name) {
        if (id.seq == ROOT_SCHEMA_SEQ) {
            if (name.equals(META_NAME)) {
                return META_SCHEMA;
            }
            if (name.equals(DINGO_NAME)) {
                return ROOT_SCHEMA;
            }
        }
        return getMetaAdaptor(Schema.class).getByDomain(id.seq).stream()
            .filter(__ -> __.getName().equals(name)).findAny().orElse(null);
    }

    @Override
    public boolean dropSchema(CommonId id) {
        if (id.seq == META_SCHEMA_SEQ || id.seq == ROOT_SCHEMA_SEQ || id.seq == DINGO_SCHEMA_SEQ) {
            throw new UnsupportedOperationException("Root, meta, dingo schema cannot drop.");
        }
        getMetaAdaptor(Schema.class).delete(id);
        schemaListener.remove(id);
        listenService.unregister(id, SCHEMA);
        return true;
    }

    @Override
    public CommonId createTable(
        CommonId schemaId, @NonNull String tableName, @NonNull TableDefinition tableDefinition
    ) {
        if (schemaId.seq == META_SCHEMA_SEQ || schemaId.seq == ROOT_SCHEMA_SEQ) {
            throw new UnsupportedOperationException("Root schema and meta schema cannot create table.");
        }
        if (adaptor.getDefinition(schemaId, tableName) != null) {
            throw new RuntimeException("Table " + tableName + " already exists");
        }
        CommonId tableId = adaptor.create(schemaId, tableDefinition);
        schemaListener.get(schemaId).accept(
            new Message(ProtostuffCodec.write(new MetaListenEvent(CREATE_TABLE, adaptor.get(tableId))))
        );
        return tableId;
    }

    @Override
    public synchronized boolean dropTable(CommonId schemaId, @NonNull String tableName) {
        if (schemaId.seq == META_SCHEMA_SEQ || schemaId.seq == ROOT_SCHEMA_SEQ) {
            throw new UnsupportedOperationException("Root schema and meta schema cannot drop table.");
        }
        CommonId tableId = adaptor.getTableId(schemaId, tableName);
        boolean result = tableId != null && adaptor.delete(schemaId, tableName);
        if (result) {
            schemaListener.get(schemaId).accept(
                new Message(ProtostuffCodec.write(new MetaListenEvent(DELETE_TABLE, tableName)))
            );
        }
        return result;
    }

    @Override
    public CommonId getTableId(@NonNull CommonId id, @NonNull String tableName) {
        return adaptor.getTableId(id, tableName);
    }

    @Override
    public List<Table> getTableMetas(CommonId schemaId) {
        if (schemaId.seq == META_SCHEMA_SEQ) {
            return MetaAdaptorRegistry.getAll().stream().map(
                adaptor -> Table.builder()
                    .id(adaptor.id()).name(adaptor.getDefinition().getName()).definition(adaptor.getDefinition())
                    .build()
            ).collect(Collectors.toList());
        }
        return adaptor.getAll(schemaId);
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions(@NonNull CommonId id) {
        if (id.seq == META_SCHEMA_SEQ) {
            return Constant.ADAPTOR_DEFINITION_MAP.values().stream()
                .collect(Collectors.toMap(TableDefinition::getName, Function.identity()));
        }
        return adaptor.getAllDefinition(id);
    }

    @Override
    public Table getTableMeta(CommonId schemaId, String name) {
        return adaptor.get(schemaId, name);
    }

    @Override
    public Table getTableMeta(CommonId tableId) {
        return adaptor.get(tableId);
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull CommonId id, @NonNull String name) {
        return adaptor.getDefinition(id, name);
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull CommonId commonId) {
        return adaptor.getDefinition(commonId);
    }

    @Override
    public Set<Location> getTableDistribute(CommonId tableId) {
        if (tableId.domain == META_SCHEMA_SEQ) {
            return new HashSet<>(Configuration.servers());
        }
        return new HashSet<>(getMetaAdaptor(Table.class).get(tableId).getLocations().values());
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts(@NonNull CommonId id, String name) {
        throw new UnsupportedOperationException();
    }

}
