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

package io.dingodb.raft.storage.snapshot;

import io.dingodb.raft.Lifecycle;
import io.dingodb.raft.entity.RaftOutter.SnapshotMeta;

import java.io.Closeable;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class SnapshotReader extends Snapshot implements Closeable, Lifecycle<Void> {
    /**
     * Load the snapshot metadata.
     */
    public abstract SnapshotMeta load();

    /**
     * Generate uri for other peers to copy this snapshot.
     * Return an empty string if some error has occur.
     */
    public abstract String generateURIForCopy();
}
