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

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class StoreDBOptions {
    // The raft log used fsync by default, and the correctness of
    // state-machine data depends on the raft log + snapshot,
    // so we do not need to fsync.
    /**
     * If true, the write will be flushed from the operating system
     * buffer cache (by calling WritableFile::Sync()) before the write
     * is considered complete.  If this flag is true, writes will be
     * slower.
     * If this flag is false, and the machine crashes, some recent
     * writes may be lost.  Note that if it is just the process that
     * crashes (i.e., the machine does not reboot), no writes will be
     * lost even if sync==false.
     * In other words, a DB write with sync==false has similar
     * crash semantics as the "write()" system call.  A DB write
     * with sync==true has similar crash semantics to a "write()"
     * system call followed by "fdatasync()".
     */
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
    private String  dataPath;
    private int     countInGroup                      = 10000;
}
