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

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@Setter
@ToString
public class RocksDBOptions {
    // The raft log used fsync by default, and the correctness of
    // state-machine data with dingokv depends on the raft log + snapshot,
    // so we do not need to fsync.
    private boolean sync                              = false;
    // For the same reason(See the comment of ‘sync’ field), we also
    // don't need WAL, which can improve performance.
    //
    // If `sync` is true, `disableWAL` must be set false
    private boolean disableWAL                        = true;
    // https://github.com/facebook/rocksdb/wiki/Checkpoints
    private boolean fastSnapshot                      = false;
    private boolean asyncSnapshot                     = false;
    // Statistics to analyze the performance of db
    private boolean openStatisticsCollector           = true;
    private long    statisticsCallbackIntervalSeconds = 0;
    private String  dbPath;
}
