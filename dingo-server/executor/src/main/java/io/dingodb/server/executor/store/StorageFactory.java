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

package io.dingodb.server.executor.store;

import io.dingodb.mpu.storage.Storage;
import io.dingodb.mpu.storage.mem.MemStorage;
import io.dingodb.mpu.storage.rocks.RocksStorage;
import io.dingodb.server.executor.config.Configuration;

import java.nio.file.Path;

public final class StorageFactory {

    private StorageFactory() {
    }

    public static Storage create(String label, Path path) throws Exception {
        return create(label, path, 0);
    }

    public static Storage create(String label, Path path, int ttl) throws Exception {
        if (Configuration.isMem()) {
            return new MemStorage();
        }
        return new RocksStorage(label, path, ttl, ttl > 0);
    }

}
