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

package io.dingodb.raft;

import io.dingodb.raft.entity.codec.LogEntryCodecFactory;
import io.dingodb.raft.option.NodeOptions;
import io.dingodb.raft.option.RaftOptions;
import io.dingodb.raft.storage.LogStorage;
import io.dingodb.raft.storage.LogStore;
import io.dingodb.raft.storage.RaftMetaStorage;
import io.dingodb.raft.storage.SnapshotStorage;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface JRaftServiceFactory {
    /**
     * Creates a raft log storage.
     * @param regionId Region Id Str.
     * @param logStore the log store to.
     * @return storage to store raft log entires.
     */
    LogStorage createLogStorage(final String regionId, LogStore logStore);

    /**
     * Creates a raft snapshot storage
     * @param uri  The snapshot storage uri from {@link NodeOptions#getSnapshotUri()}
     * @param raftOptions  the raft options.
     * @return storage to store state machine snapshot.
     */
    SnapshotStorage createSnapshotStorage(final String uri, final RaftOptions raftOptions);

    /**
     * Creates a raft meta storage.
     * @param uri  The meta storage uri from {@link NodeOptions#getRaftMetaUri()}
     * @param raftOptions  the raft options.
     * @return meta storage to store raft meta info.
     */
    RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions);

    /**
     * Creates a log entry codec factory.
     * @return a codec factory to create encoder/decoder for raft log entry.
     */
    LogEntryCodecFactory createLogEntryCodecFactory();
}
