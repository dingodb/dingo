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
import io.dingodb.common.Location;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.meta.Part;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

public class MetaService implements io.dingodb.meta.MetaService {

    public static final CommonId ROOT_ID = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.schema, 1, 1);
    public static final String ROOT_NAME = "LOCAL_ROOT";
    public static final MetaService ROOT = new MetaService(ROOT_ID, ROOT_NAME);
    private static final NavigableMap<CommonId, MetaService> metaServices = new ConcurrentSkipListMap<>();
    private static final NavigableMap<CommonId, TableDefinition> tableDefinitions = new ConcurrentSkipListMap<>();
    private static final Map<CommonId, NavigableMap<ComparableByteArray, Part>> parts = new ConcurrentSkipListMap<>();
    private static final AtomicInteger metaServiceSeq = new AtomicInteger(1);
    private static final AtomicInteger tableSeq = new AtomicInteger(1);
    private static Location location;
    private static NavigableMap<ComparableByteArray, Part> defaultPart;

    private final CommonId id;
    private final String name;

    private MetaService(CommonId id, String name) {
        this.id = id;
        this.name = name;
    }

    public static void clear() {
        metaServices.clear();
        tableDefinitions.clear();
        parts.clear();
    }

    public static void setLocation(Location location) {
        MetaService.location = location;
    }

    @Override
    public CommonId id() {
        return id;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void createSubMetaService(String name) {
        CommonId newId = new CommonId(id.type(), id.identifier(), id.seq(), metaServiceSeq.incrementAndGet());
        metaServices.put(newId, new MetaService(newId, name));
    }

    @Override
    public Map<String, io.dingodb.meta.MetaService> getSubMetaServices() {
        return metaServices.subMap(
                CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.schema, id.seq()), true,
                CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.schema, id.seq() + 1), false
            ).values().stream()
            .collect(Collectors.toMap(MetaService::name, __ -> __));
    }

    @Override
    public io.dingodb.meta.MetaService getSubMetaService(String name) {
        return getSubMetaServices().get(name);
    }

    @Override
    public boolean dropSubMetaService(String name) {
        metaServices.remove(getSubMetaService(name).id());
        return true;
    }

    @Override
    public void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
        CommonId tableId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.table, id.seq(), tableSeq.incrementAndGet());
        tableDefinitions.put(tableId, tableDefinition);
    }

    @Override
    public boolean dropTable(@NonNull String tableName) {
        tableName = tableName.toUpperCase();
        tableDefinitions.remove(getTableId(tableName));
        return true;
    }

    @Override
    public CommonId getTableId(@NonNull String tableName) {
        String tableNameU = tableName.toUpperCase();
        return tableDefinitions.subMap(
                CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.table, id.seq()), true,
                CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.table, id.seq() + 1), false
            ).entrySet().stream()
            .filter(e -> e.getValue().getName().equals(tableNameU))
            .findAny().map(Map.Entry::getKey).orElse(null);
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        return tableDefinitions.subMap(
                CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.table, id.seq()), true,
                CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.table, id.seq() + 1), false
            ).values().stream()
            .collect(Collectors.toMap(TableDefinition::getName, __ -> __));
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull String tableName) {
        tableName = tableName.toUpperCase();
        return getTableDefinitions().get(tableName);
    }

    public TableDefinition getTableDefinition(@NonNull CommonId id) {
        return null;
    }

    @Override
    public NavigableMap<ComparableByteArray, Part> getParts(String tableName) {
        tableName = tableName.toUpperCase();
        return Parameters.cleanNull(parts.get(getTableId(tableName)), defaultPart);
    }

    @Override
    public NavigableMap<ComparableByteArray, Part> getParts(CommonId id) {
        return Parameters.cleanNull(parts.get(id), defaultPart);
    }

    @Override
    public Location currentLocation() {
        return location;
    }

    @Override
    public void createIndex(String tableName, List<Index> indexList) {

    }

    @Override
    public void dropIndex(String tableName, String indexName) {

    }

    @Override
    public TableDefinition getIndexTableDefinition(String tableName) {
        return null;
    }

    public void setParts(CommonId id, NavigableMap<ComparableByteArray, Part> part) {
        parts.put(id, part);
    }

    public void setParts(NavigableMap<ComparableByteArray, Part> part) {
        defaultPart = part;
    }

    @AutoService(MetaServiceProvider.class)
    public static class Provider implements MetaServiceProvider {
        @Override
        public io.dingodb.meta.MetaService root() {
            return ROOT;
        }
    }

}
