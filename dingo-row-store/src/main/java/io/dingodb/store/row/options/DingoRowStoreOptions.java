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

package io.dingodb.store.row.options;

import io.dingodb.raft.util.Utils;
import io.dingodb.store.row.options.configured.BatchingOptionsConfigured;
import io.dingodb.store.row.options.configured.RpcOptionsConfigured;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@Setter
@ToString
public class DingoRowStoreOptions {
    // A clusterId is required to connect to the PD server, and PD server
    // use clusterId isolate different cluster.  The fake PD mode does not
    // need to be configured.
    private long                   clusterId = 0;
    // Each store node contains one or more raft-group replication groups.
    // This field is the name prefix of all replication groups. All raft-group
    // names follow the naming rules of [clusterName-regionId].
    private String                 clusterName           = "default-group-cluster";
    private PlacementDriverOptions placementDriverOptions;
    private StoreEngineOptions     storeEngineOptions;
    // Initial server node list.
    private String                 initialServerList;
    // Whether to read data only from the leader node, reading from the
    // follower node can also ensure consistent reading, but the reading
    // delay may increase due to the delay of the follower synchronization
    // data.
    private boolean                onlyLeaderRead        = true;
    private RpcOptions             rpcOptions            = RpcOptionsConfigured.newDefaultConfig();
    private int                    failoverRetries       = 2;
    private long                   futureTimeoutMillis   = 5000;
    private boolean                useParallelKVExecutor = true;
    private BatchingOptions        batchingOptions       = BatchingOptionsConfigured.newDefaultConfig();
    // If 'useParallelCompress' is true , We will compress and decompress Snapshot concurrently
    private boolean                useParallelCompress   = false;
    private int                    compressThreads       = Utils.cpus();
    private int                    deCompressThreads     = Utils.cpus() + 1;
}
