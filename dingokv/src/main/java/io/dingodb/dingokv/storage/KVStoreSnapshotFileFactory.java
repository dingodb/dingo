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

package io.dingodb.dingokv.storage;

import com.alipay.sofa.jraft.util.Requires;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class KVStoreSnapshotFileFactory {
    public static <T> KVStoreSnapshotFile getKVStoreSnapshotFile(final BaseRawKVStore<T> kvStore) {
        Requires.requireNonNull(kvStore, "kvStore");
        if (kvStore instanceof RocksRawKVStore) {
            return new RocksKVStoreSnapshotFile((RocksRawKVStore) kvStore);
        }
        if (kvStore instanceof MemoryRawKVStore) {
            return new MemoryKVStoreSnapshotFile((MemoryRawKVStore) kvStore);
        }
        throw reject("fail to find a KVStoreSnapshotFile with " + kvStore.getClass().getName());
    }

    private static UnsupportedOperationException reject(final String message) {
        return new UnsupportedOperationException(message);
    }

    private KVStoreSnapshotFileFactory() {
    }
}
