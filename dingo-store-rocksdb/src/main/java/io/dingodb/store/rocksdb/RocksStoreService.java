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

package io.dingodb.store.rocksdb;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import org.rocksdb.RocksDB;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

@AutoService(StoreService.class)
public class RocksStoreService implements StoreService {
    public static final RocksStoreService INSTANCE = new RocksStoreService();

    static {
        RocksDB.loadLibrary();
    }

    private final Map<CommonId, StoreInstance> locationStoreInstanceMap = new ConcurrentHashMap<>();

    private RocksStoreService() {
    }

    @Override
    public String name() {
        return "ROCKSDB";
    }

    @Override
    public StoreInstance getOrCreateInstance(@Nonnull CommonId id) {
        return getInstance(id);
    }

    @Override
    public void deleteInstance(CommonId id) {

    }

    @Override
    public StoreInstance getInstance(@Nonnull CommonId id) {
        return locationStoreInstanceMap.compute(id, (l, i) -> i == null ? new RocksStoreInstance(id) : i);
    }
}
