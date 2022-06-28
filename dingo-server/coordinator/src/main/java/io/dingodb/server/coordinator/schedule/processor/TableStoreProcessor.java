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

package io.dingodb.server.coordinator.schedule.processor;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.store.Part;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.TableStoreApi;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.TablePart;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class TableStoreProcessor {

    public static final ApiRegistry API_REGISTRY = ApiRegistry.getDefault();

    private static final Map<CommonId, TableStoreApi> APIS = new ConcurrentHashMap<>();

    private TableStoreProcessor() {
    }

    public static TableStoreApi addStore(CommonId id, Location location) {
        return APIS.computeIfAbsent(id, any -> API_REGISTRY.proxy(TableStoreApi.class, () -> location));
    }

    public static TableStoreApi getOrAddStore(CommonId id) {
        return APIS.computeIfAbsent(id, any -> API_REGISTRY
            .proxy(TableStoreApi.class, () -> MetaAdaptorRegistry.getMetaAdaptor(Executor.class).get(id).location())
        );
    }

    public static TableStoreApi getStore(CommonId id) {
        return APIS.get(id);
    }

    public static void deleteTable(CommonId tableId) {
        APIS.values().forEach(api -> Executors.execute("delete-table", () -> api.deleteTable(tableId)));
    }

    public static void addReplica(CommonId executor, CommonId table, CommonId part, Location replica) {
        TableStoreApi api = getOrAddStore(executor);
        api.addTablePartReplica(table, part, replica);
    }

    public static void removeReplica(CommonId executor, TablePart tablePart) {
        TableStoreApi api = getOrAddStore(executor);
        api.deleteTablePart(Part.builder()
            .id(tablePart.getId())
            .instanceId(tablePart.getTable())
            .type(Part.PartType.ROW_STORE)
            .start(tablePart.getStart())
            .end(tablePart.getEnd())
            .build());
    }

    public static void applyTablePart(
        TablePart tablePart, CommonId executor, List<Location> replicaLocations, boolean exist
    ) {
        applyTablePart(tablePart, executor, replicaLocations, null, exist);
    }

    public static void applyTablePart(
        TablePart tablePart, CommonId executor, List<Location> replicaLocations, Location leader, boolean exist
    ) {
        TableStoreApi api = APIS.get(executor);
        if (api == null) {
            api = addStore(executor, MetaAdaptorRegistry.getMetaAdaptor(Executor.class).get(executor).location());
        }
        Part part = Part.builder()
            .id(tablePart.getId())
            .instanceId(tablePart.getTable())
            .type(Part.PartType.ROW_STORE)
            .start(tablePart.getStart())
            .end(tablePart.getEnd())
            .leader(leader)
            .replicates(replicaLocations)
            .build();
        log.info("Apply part [{}] on [{}], part info: {}", tablePart.getId(), executor, part);
        if (exist) {
            api.reassignTablePart(part);
        } else {
            api.assignTablePart(part);
        }
    }

}
