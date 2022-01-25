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

import com.google.protobuf.Message;
import io.dingodb.raft.Lifecycle;
import io.dingodb.raft.entity.RaftOutter.SnapshotMeta;

import java.io.Closeable;
import java.io.IOException;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class SnapshotWriter extends Snapshot implements Closeable, Lifecycle<Void> {
    /**
     * Save a snapshot metadata.
     *
     * @param meta snapshot metadata
     * @return true on success
     */
    public abstract boolean saveMeta(final SnapshotMeta meta);

    /**
     * Adds a snapshot file without metadata.
     *
     * @param fileName file name
     * @return true on success
     */
    public boolean addFile(final String fileName) {
        return addFile(fileName, null);
    }

    /**
     * Adds a snapshot file with metadata.
     *
     * @param fileName file name
     * @param fileMeta file metadata
     * @return true on success
     */
    public abstract boolean addFile(final String fileName, final Message fileMeta);

    /**
     * Remove a snapshot file.
     *
     * @param fileName file name
     * @return true on success
     */
    public abstract boolean removeFile(final String fileName);

    /**
     * Close the writer.
     *
     * @param keepDataOnError whether to keep data when error happens.
     * @throws IOException if occurred an IO error
     */
    public abstract void close(final boolean keepDataOnError) throws IOException;
}
