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

package io.dingodb.store.row.storage;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import io.dingodb.store.row.metadata.Region;

import java.util.concurrent.ExecutorService;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface KVStoreSnapshotFile {
    /**
     * Save a snapshot for the specified region.
     *
     * @param writer   snapshot writer
     * @param region   the region to save snapshot
     * @param done     callback
     * @param executor the executor to compress snapshot
     */
    void save(final SnapshotWriter writer, final Region region, final Closure done, final ExecutorService executor);

    /**
     * Load snapshot for the specified region.
     *
     * @param reader snapshot reader
     * @param region the region to load snapshot
     * @return true if load succeed
     */
    boolean load(final SnapshotReader reader, final Region region);
}
