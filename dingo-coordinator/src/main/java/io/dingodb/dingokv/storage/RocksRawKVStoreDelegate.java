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

import io.dingodb.dingokv.storage.RocksDBBackupInfo;
import io.dingodb.dingokv.storage.RocksRawKVStore;
import lombok.experimental.Delegate;

import java.io.IOException;

public class RocksRawKVStoreDelegate {

    @Delegate
    private final RocksRawKVStore rocksRawKVStore;

    public RocksRawKVStoreDelegate(RocksRawKVStore rocksRawKVStore) {
        this.rocksRawKVStore = rocksRawKVStore;
    }

    public void writeSnapshot(final String snapshotPath) {
        rocksRawKVStore.writeSnapshot(snapshotPath);
    }

    public void readSnapshot(final String snapshotPath) {
        rocksRawKVStore.readSnapshot(snapshotPath);
    }

    public void restoreBackup(final String backupPath, final RocksDBBackupInfo rocksBackupInfo) {
        rocksRawKVStore.restoreBackup(backupPath, rocksBackupInfo);
    }

    public RocksDBBackupInfo backupDB(final String backupPath) throws IOException {
        return rocksRawKVStore.backupDB(backupPath);
    }

    public boolean isFastSnapshot() {
        return rocksRawKVStore.isFastSnapshot();
    }

}
