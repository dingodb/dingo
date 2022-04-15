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
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

@AutoService(StoreService.class)
public class MemoryStoreService implements StoreService {

    private final Map<CommonId, MemoryStoreInstance> store = new ConcurrentHashMap<>();

    @Override
    public String name() {
        return "MEMORY";
    }

    @Override
    public StoreInstance getInstance(@Nonnull CommonId id) {
        MemoryStoreInstance instance = store.get(id);
        if (instance == null) {
            instance = new MemoryStoreInstance();
            store.put(id, instance);
        }
        return instance;
    }

    @Override
    public void deleteInstance(CommonId id) {
        store.remove(id);
    }
}
