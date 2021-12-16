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

package io.dingodb.dingokv.storage;

import org.rocksdb.BackupInfo;

import java.io.Serializable;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class RocksDBBackupInfo implements Serializable {
    private static final long serialVersionUID = -7010741841443565098L;

    private int               backupId;
    private int               numberFiles;
    private long              timestamp;
    private long              size;

    public RocksDBBackupInfo(BackupInfo info) {
        super();
        this.size = info.size();
        this.backupId = info.backupId();
        this.timestamp = info.timestamp();
        this.numberFiles = info.numberFiles();
    }

    public int getBackupId() {
        return backupId;
    }

    public void setBackupId(int backupId) {
        this.backupId = backupId;
    }

    public RocksDBBackupInfo() {
        super();
    }

    public int getNumberFiles() {
        return numberFiles;
    }

    public void setNumberFiles(int numberFiles) {
        this.numberFiles = numberFiles;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "RocksDBBackupInfo{" + "backupId=" + backupId + ", numberFiles=" + numberFiles + ", timestamp="
               + timestamp + ", size=" + size + '}';
    }
}
