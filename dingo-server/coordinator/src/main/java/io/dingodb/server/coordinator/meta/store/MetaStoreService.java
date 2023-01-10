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

package io.dingodb.server.coordinator.meta.store;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.net.service.ListenService;
import io.dingodb.server.coordinator.meta.adaptor.Adaptor;
import io.dingodb.server.protocol.ListenerTags;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.api.StoreServiceProvider;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;

public class MetaStoreService implements StoreService {
    public static final MetaStoreService STORE_SERVICE = new MetaStoreService();

    @AutoService(StoreServiceProvider.class)
    public static class Provider implements StoreServiceProvider {

        @Override
        public StoreService get() {
            return STORE_SERVICE;
        }
    }

    private static final Map<CommonId, StoreInstance> storeInstances = new HashMap<>();
    private static final ListenService listenService = ListenService.getDefault();

    private MetaStoreService() {
    }

    public void add(Adaptor<?> adaptor) {
        storeInstances.put(adaptor.id(), adaptor);
        listenService.register(adaptor.id(), ListenerTags.MetaListener.TABLE_DEFINITION);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public StoreInstance getOrCreateInstance(@NonNull CommonId id, int ttl) {
        return storeInstances.get(id);
    }

    @Override
    public StoreInstance getInstance(@NonNull CommonId id) {
        return storeInstances.get(id);
    }

    @Override
    public void deleteInstance(CommonId id) {
        throw new UnsupportedOperationException();
    }
}
