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

package io.dingodb.server.coordinator.meta.service;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.Part;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.coordinator.meta.adaptor.impl.ExecutorAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.ReplicaAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TableAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TablePartStatsAdaptor;
import io.dingodb.server.protocol.CommonIdConstant;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.Schema;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.server.protocol.meta.TablePartStats;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.common.util.Parameters.cleanNull;
import static io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry.getMetaAdaptor;
import static io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry.getStatsMetaAdaptor;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.ROOT_DOMAIN;
import static io.dingodb.server.protocol.CommonIdConstant.STATS_IDENTIFIER;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

@Slf4j
public class DingoMetaService implements MetaService, MetaServiceApi {

    public static final DingoMetaService ROOT = new DingoMetaService(null, ROOT_NAME);
    public static final CommonId ROOT_ID = new CommonId(
        ID_TYPE.table, TABLE_IDENTIFIER.schema, ROOT_DOMAIN, ROOT_DOMAIN
    );

    static {
        ApiRegistry.getDefault().register(MetaServiceApi.class, ROOT);
    }

    public final CommonId id;
    public final String name;

    private DingoMetaService(CommonId id, String name) {
        this.id = id;
        this.name = name;
    }

    private int domain(CommonId id) {
        return Optional.mapOrGet(id, CommonId::domain, () -> CommonIdConstant.ROOT_DOMAIN);
    }

    @Override
    public CommonId rootId() {
        return ROOT_ID;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Location currentLocation() {
        return DingoConfiguration.location();
    }

    @Override
    public CommonId id() {
        return id;
    }

    @Override
    public void createSubMetaService(CommonId id, String name) {
        getMetaAdaptor(Schema.class).save(Schema.builder().name(name).build());
    }

    @Override
    public Map<String, MetaService> getSubMetaServices(CommonId id) {
        return getMetaAdaptor(Schema.class).getByDomain(domain(id)).stream()
            .map(schema -> new DingoMetaService(schema.getId(), schema.getName()))
            .collect(Collectors.toMap(__ -> __.name, __ -> __));
    }

    @Override
    public MetaService getSubMetaService(CommonId id, String name) {
        Schema schema = getSubSchema(id, name);
        return new DingoMetaService(schema.getId(), schema.getName());
    }

    @Override
    public boolean dropSubMetaService(CommonId id, String name) {
        return dropSchema(id, name);
    }

    @Override
    public Map<String, Schema> getSubSchemas(CommonId id) {
        return getMetaAdaptor(Schema.class).getByDomain(domain(id)).stream()
            .collect(Collectors.toMap(Schema::getName, Function.identity()));
    }

    @Override
    public Schema getSubSchema(CommonId id, String name) {
        return getMetaAdaptor(Schema.class).getByDomain(domain(id)).stream()
            .filter(__ -> __.getName().equals(name)).findAny().orElse(null);
    }

    @Override
    public boolean dropSchema(CommonId id, String name) {
        getMetaAdaptor(Schema.class).delete(Optional.mapOrNull(getSubSchema(id, name), Schema::getId));
        return true;
    }

    @Override
    public synchronized void createTable(
        CommonId id, @NonNull String tableName, @NonNull TableDefinition tableDefinition
    ) {
        if (getTableDefinitions().containsKey(tableName)) {
            throw new RuntimeException("Table " + tableName + " already exists");
        }
        ((TableAdaptor) getMetaAdaptor(Table.class)).create(cleanNull(id, ROOT_ID), tableDefinition);
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
        return ((TableAdaptor) getMetaAdaptor(Table.class)).getDefinition(getTableId(cleanNull(id, ROOT_ID), name));
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull CommonId commonId) {
        return ((TableAdaptor) getMetaAdaptor(Table.class)).getDefinition(commonId);
    }

    @Override
    public NavigableMap<ComparableByteArray, Part> getParts(@NonNull CommonId id, String name) {
        CommonId tableId = ((TableAdaptor) getMetaAdaptor(Table.class)).getTableId(cleanNull(id, ROOT_ID), name);

        TablePartStatsAdaptor statsMetaAdaptor = getStatsMetaAdaptor(TablePartStats.class);
        ExecutorAdaptor executorAdaptor = getMetaAdaptor(Executor.class);
        ReplicaAdaptor replicaAdaptor = getMetaAdaptor(Replica.class);

        NavigableMap<ComparableByteArray, Part> result = new TreeMap<>();
        getMetaAdaptor(TablePart.class).getByDomain(tableId.seq()).forEach(part -> {
            Optional.ofNullable(statsMetaAdaptor.getStats(
                new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, part.getId().domain(),
                    part.getId().seq())
            )).ifPresent(stats -> {
                result.put(
                    new ComparableByteArray(part.getStart()),
                    new Part(
                        part.getId().encode(),
                        executorAdaptor.getLocation(stats.getLeader()),
                        replicaAdaptor.getLocationsByDomain(part.getId().seq()),
                        part.getStart(),
                        part.getEnd())
                );
            });
        });
        return result;
    }

}
