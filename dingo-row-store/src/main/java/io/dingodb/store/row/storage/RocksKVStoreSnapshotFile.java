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

package io.dingodb.store.row.storage;

import io.dingodb.store.row.errors.StorageException;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.store.row.util.RegionHelper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static io.dingodb.raft.entity.LocalFileMetaOutter.LocalFileMeta;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class RocksKVStoreSnapshotFile extends AbstractKVStoreSnapshotFile {
    private final RocksRawKVStore kvStore;

    RocksKVStoreSnapshotFile(RocksRawKVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    CompletableFuture<LocalFileMeta.Builder> doSnapshotSave(final String snapshotPath, final Region region,
                                                            final ExecutorService executor) throws Exception {
        if (RegionHelper.isMultiGroup(region)) {
            final CompletableFuture<Void> snapshotFuture = this.kvStore.writeSstSnapshot(snapshotPath, region, executor);
            final CompletableFuture<LocalFileMeta.Builder> metaFuture = new CompletableFuture<>();
            snapshotFuture.whenComplete((aVoid, throwable) -> {
                if (throwable == null) {
                    metaFuture.complete(writeMetadata(region));
                } else {
                    metaFuture.completeExceptionally(throwable);
                }
            });
            return metaFuture;
        }
        if (this.kvStore.isFastSnapshot()) {
            // Checkpoint is fast enough, no need to asynchronous
            this.kvStore.writeSnapshot(snapshotPath);
            return CompletableFuture.completedFuture(writeMetadata(null));
        }
        final RocksDBBackupInfo backupInfo = this.kvStore.backupDB(snapshotPath);
        return CompletableFuture.completedFuture(writeMetadata(backupInfo));
    }

    @Override
    void doSnapshotLoad(final String snapshotPath, final LocalFileMeta meta, final Region region) throws Exception {
        if (RegionHelper.isMultiGroup(region)) {
            final Region snapshotRegion = readMetadata(meta, Region.class);
            if (!RegionHelper.isSameRange(region, snapshotRegion)) {
                throw new StorageException("Invalid snapshot region: " + snapshotRegion + " current region is: "
                                           + region);
            }
            this.kvStore.readSstSnapshot(snapshotPath);
            return;
        }
        if (this.kvStore.isFastSnapshot()) {
            this.kvStore.readSnapshot(snapshotPath);
            return;
        }
        final RocksDBBackupInfo rocksBackupInfo = readMetadata(meta, RocksDBBackupInfo.class);
        this.kvStore.restoreBackup(snapshotPath, rocksBackupInfo);
    }
}
