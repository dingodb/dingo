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
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.coordinator.meta.adaptor.impl.TableAdaptor;
import io.dingodb.server.protocol.meta.Schema;
import io.dingodb.server.protocol.meta.Table;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.common.util.Parameters.cleanNull;
import static io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry.getMetaAdaptor;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.ROOT_DOMAIN;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

public class MetaServiceApi implements io.dingodb.server.api.MetaServiceApi {

    public static final MetaServiceApi INSTANCE;

    public static final CommonId ROOT_ID = new CommonId(
        ID_TYPE.table, TABLE_IDENTIFIER.schema, ROOT_DOMAIN, ROOT_DOMAIN
    );

    static {
        ApiRegistry.getDefault().register(io.dingodb.server.api.MetaServiceApi.class, INSTANCE = new MetaServiceApi());
    }

    private MetaServiceApi() {
    }

    @Override
    public CommonId rootId() {
        return ROOT_ID;
    }

    @Override
    public CommonId createSubMetaService(CommonId id, String name) {
        return getMetaAdaptor(Schema.class).save(Schema.builder().name(name).parent(id).build());
    }

    @Override
    public Map<String, Schema> getSubSchemas(CommonId id) {
        return getMetaAdaptor(Schema.class).getByDomain(id.domain).stream()
            .collect(Collectors.toMap(Schema::getName, Function.identity()));
    }

    @Override
    public Schema getSubSchema(CommonId id, String name) {
        return getMetaAdaptor(Schema.class).getByDomain(id.domain).stream()
            .filter(__ -> __.getName().equals(name)).findAny().orElse(null);
    }

    @Override
    public boolean dropSchema(CommonId id) {
        getMetaAdaptor(Schema.class).delete(id);
        return true;
    }

    @Override
    public synchronized CommonId createTable(
        CommonId id, @NonNull String tableName, @NonNull TableDefinition tableDefinition
    ) {
        TableAdaptor tableAdaptor = getMetaAdaptor(Table.class);
        if (tableAdaptor.get(id, tableName) != null) {
            throw new RuntimeException("Table " + tableName + " already exists");
        }
        return tableAdaptor.create(cleanNull(id, ROOT_ID), tableDefinition);
    }

    @Override
    public synchronized boolean dropTable(CommonId id, @NonNull String tableName) {
        CommonId tableId = ((TableAdaptor) getMetaAdaptor(Table.class)).getTableId(cleanNull(id ,ROOT_ID), tableName);
        return tableId != null && ((TableAdaptor) getMetaAdaptor(Table.class)).delete(id, tableName);
    }

    @Override
    public CommonId getTableId(@NonNull CommonId id, @NonNull String tableName) {
        return ((TableAdaptor) getMetaAdaptor(Table.class)).getTableId(cleanNull(id, ROOT_ID), tableName);
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions(@NonNull CommonId id) {
        return ((TableAdaptor) getMetaAdaptor(Table.class)).getAllDefinition(cleanNull(id, ROOT_ID));
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull CommonId id, @NonNull String name) {
        return ((TableAdaptor) getMetaAdaptor(Table.class)).get(cleanNull(id, ROOT_ID), name);
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull CommonId commonId) {
        return ((TableAdaptor) getMetaAdaptor(Table.class)).getDefinition(commonId);
    }

    @Override
    public Set<Location> getTableDistribute(CommonId tableId) {
        return new HashSet<>(getMetaAdaptor(Table.class).get(tableId).getLocations().values());
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts(@NonNull CommonId id, String name) {
        throw new UnsupportedOperationException();
    }

}
