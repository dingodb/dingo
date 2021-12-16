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

package io.dingodb.dingokv.options;

import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Utils;
import io.dingodb.dingokv.storage.StorageType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.concurrent.TimeUnit;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@Setter
@ToString
public class StoreEngineOptions {
    private StorageType               storageType                   = StorageType.RocksDB;
    private RocksDBOptions            rocksDBOptions;
    private MemoryDBOptions           memoryDBOptions;
    private String                    raftDataPath;
    private Endpoint serverAddress;
    // Most configurations do not need to be configured separately for each raft-group,
    // so a common configuration is provided, and each raft-group can copy from here.
    private NodeOptions commonNodeOptions             = new NodeOptions();
    private List<RegionEngineOptions> regionEngineOptionsList;
    private String                    initialServerList;
    private HeartbeatOptions          heartbeatOptions;
    private boolean                   useSharedRpcExecutor;
    // thread poll number of threads
    private int                       readIndexCoreThreads          = Math.max(Utils.cpus() << 2, 16);
    private int                       leaderStateTriggerCoreThreads = 4;
    private int                       snapshotCoreThreads           = 1;
    private int                       snapshotMaxThreads            = 32;
    private int                       cliRpcCoreThreads             = Utils.cpus() << 2;
    private int                       raftRpcCoreThreads            = Math.max(Utils.cpus() << 3, 32);
    private int                       kvRpcCoreThreads              = Math.max(Utils.cpus() << 3, 32);
    // metrics schedule option (seconds), won't start reporter id metricsReportPeriod <= 0
    private long                      metricsReportPeriod           = TimeUnit.MINUTES.toSeconds(5);
    // the minimum number of keys required to split, less than this value will refuse to split
    private long                      leastKeysOnSplit              = 10000;
}
