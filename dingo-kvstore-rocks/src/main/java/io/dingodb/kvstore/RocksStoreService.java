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

package io.dingodb.kvstore;

import org.rocksdb.RocksDB;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

public class RocksStoreService implements KvStoreService {
    public static final RocksStoreService INSTANCE = new RocksStoreService();

    static {
        RocksDB.loadLibrary();
    }

    private final Map<String, KvStoreInstance> locationStoreInstanceMap = new ConcurrentHashMap<>();

    private RocksStoreService() {
    }

    @Override
    public KvStoreInstance getInstance(@Nonnull String path) {
        return locationStoreInstanceMap.compute(path, (l, i) -> i == null ? new RocksStoreInstance(path) : i);
    }
}
