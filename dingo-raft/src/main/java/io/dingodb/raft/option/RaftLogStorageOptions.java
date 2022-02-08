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

package io.dingodb.raft.option;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RaftLogStorageOptions {
    /**
     * options for database.
     */
    private long dbMaxTotalWalSize             = 0;
    private long dbRecycleLogFileNum           = 0;
    private long dbKeepLogFileNum              = 0;
    private long dbWriteBufferSize             = 0;
    private int  dbMaxBackGroupJobs            = 0;
    private int  dbMaxBackGroupFlushes         = 0;
    private int  dbMaxBackGroupCompactions     = 0;
    private long dbMaxManifestFileSize         = 0;
    private int  dbMaxSubCompactions           = 0;

    /**
     * options for column family.
     */
    private long cfWriteBufferSize             = 0;
    private long cfMaxCompactionBytes          = 0;
    private int  cfMaxWriteBufferNumber        = 0;
    private int  cfMinWriteBufferNumberToMerge = 0;
    private long cfArenaBlockSize              = 0;
    private long cfBlockSize                   = 0;
    private long cfBlockCacheSize              = 0;

    @Override
    public String toString() {
        return "RaftLogStorageOptions{" + "dbMaxTotalWalSize=" + dbMaxTotalWalSize + ", dbRecycleLogFileNum="
            + dbRecycleLogFileNum + ", dbKeepLogFileNum=" + dbKeepLogFileNum + ", dbWriteBufferSize="
            + dbWriteBufferSize + ", dbMaxBackGroupJobs=" + dbMaxBackGroupJobs + ", dbMaxBackGroupFlushes="
            + dbMaxBackGroupFlushes + ", dbMaxBackGroupCompactions=" + dbMaxBackGroupCompactions
            + ", dbMaxManifestFileSize=" + dbMaxManifestFileSize + ", dbMaxSubCompactions=" + dbMaxSubCompactions
            + ", cfWriteBufferSize=" + cfWriteBufferSize + ", cfMaxCompactionBytes=" + cfMaxCompactionBytes
            + ", cfMaxWriteBufferNumber=" + cfMaxWriteBufferNumber + ", cfMinWriteBufferNumberToMerge="
            + cfMinWriteBufferNumberToMerge + ", cfArenaBlockSize=" + cfArenaBlockSize + ", cfBlockSize="
            + cfBlockSize + ", cfBlockCacheSize=" + cfBlockCacheSize + '}';
    }
}
