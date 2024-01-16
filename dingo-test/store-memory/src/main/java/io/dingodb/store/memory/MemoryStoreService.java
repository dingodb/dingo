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

package io.dingodb.store.memory;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.store.Part;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@AutoService(StoreService.class)
public class MemoryStoreService implements StoreService {

    private final Map<CommonId, MemoryStoreInstance> store = new ConcurrentHashMap<>();

    @Override
    public StoreInstance getInstance(CommonId tableId, CommonId regionId) {
        if (tableId == null) {
            return null;
        }
        return store.computeIfAbsent(tableId, __ -> new MemoryStoreInstance(tableId));
    }

    @Override
    public void deleteInstance(CommonId id) {
        store.remove(id);
    }

    private Part createPart(@NonNull CommonId id, byte[] start, byte[] end) {
        return Part.builder()
            .id(id)
            .instanceId(id)
            .start(start)
            .end(end)
            .type(Part.PartType.ROW_STORE)
            .replicateLocations(Collections.emptyList())
            .leaderLocation(new Location("localhost", 0))
            .version(0)
            .build();
    }
}
