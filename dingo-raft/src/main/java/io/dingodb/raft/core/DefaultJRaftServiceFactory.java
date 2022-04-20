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

package io.dingodb.raft.core;

import io.dingodb.raft.JRaftServiceFactory;
import io.dingodb.raft.entity.codec.LogEntryCodecFactory;
import io.dingodb.raft.entity.codec.v2.LogEntryV2CodecFactory;
import io.dingodb.raft.option.RaftOptions;
import io.dingodb.raft.storage.LogStorage;
import io.dingodb.raft.storage.LogStore;
import io.dingodb.raft.storage.RaftMetaStorage;
import io.dingodb.raft.storage.SnapshotStorage;
import io.dingodb.raft.storage.impl.LocalRaftMetaStorage;
import io.dingodb.raft.storage.impl.RocksDBLogStorage;
import io.dingodb.raft.storage.impl.RocksDBLogStore;
import io.dingodb.raft.storage.snapshot.local.LocalSnapshotStorage;
import io.dingodb.raft.util.Requires;
import io.dingodb.raft.util.SPI;
import org.apache.commons.lang.StringUtils;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@SPI
public class DefaultJRaftServiceFactory implements JRaftServiceFactory {
    public static DefaultJRaftServiceFactory newInstance() {
        return new DefaultJRaftServiceFactory();
    }

    @Override
    public LogStorage createLogStorage(String regionId, LogStore logStore) {
        Requires.requireTrue(StringUtils.isNotBlank(regionId), "Blank log storage regionId.");
        Requires.requireTrue(logStore != null, "Null logStore.");
        Requires.requireTrue(logStore instanceof RocksDBLogStore, "LogStore type error.");
        return new RocksDBLogStorage(regionId, (RocksDBLogStore) logStore);
    }

    @Override
    public SnapshotStorage createSnapshotStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank snapshot storage uri.");
        return new LocalSnapshotStorage(uri, raftOptions);
    }

    @Override
    public RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank raft meta storage uri.");
        return new LocalRaftMetaStorage(uri, raftOptions);
    }

    @Override
    public LogEntryCodecFactory createLogEntryCodecFactory() {
        return LogEntryV2CodecFactory.getInstance();
    }
}
