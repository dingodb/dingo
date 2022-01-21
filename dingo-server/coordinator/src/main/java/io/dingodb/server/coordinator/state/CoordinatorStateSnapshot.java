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

package io.dingodb.server.coordinator.state;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.CRC64;
import com.alipay.sofa.jraft.util.Requires;
import com.google.protobuf.ByteString;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.store.row.serialization.Serializer;
import io.dingodb.store.row.serialization.Serializers;
import io.dingodb.store.row.storage.RawKVStore;
import io.dingodb.store.row.storage.RocksDBBackupInfo;
import io.dingodb.store.row.storage.RocksRawKVStore;
import io.dingodb.store.row.storage.zip.ZipStrategyManager;
import io.dingodb.store.row.util.StackTraceUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.zip.Checksum;

@Slf4j
public class CoordinatorStateSnapshot {

    private static final String SNAPSHOT_DIR     = "coordinator";
    private static final String BACKUP_DIR     = "coordinator";
    private static final String SNAPSHOT_ARCHIVE = "coordinator-snapshot.zip";
    private static final String BACKUP_ARCHIVE = "coordinator-snapshot.zip";

    private static final ExecutorService executor = new ThreadPoolBuilder().name("coordinator-snapshot").build();


    private final Serializer serializer       = Serializers.getDefault();

    private final RocksRawKVStore kvStore;

    public CoordinatorStateSnapshot(RocksRawKVStore kvStore) {
        this.kvStore = kvStore;
    }

    public void save(final SnapshotWriter writer, final Closure done) {
        final String writerPath = writer.getPath();
        final String snapshotPath = Paths.get(writerPath, SNAPSHOT_DIR).toString();
        try {
            saveSnapshot(snapshotPath).whenComplete((metaBuilder, throwable) -> {
                if (throwable == null) {
                    executor.execute(() -> compressSnapshot(writer, metaBuilder, done));
                } else {
                    log.error("Fail to save snapshot, path={}, file list={}, {}.", writerPath, writer.listFiles(),
                        StackTraceUtil.stackTrace(throwable));
                    done.run(new Status(
                        RaftError.EIO, "Fail to save snapshot at %s, error is %s", writerPath,
                        throwable.getMessage()));
                }
            });
        } catch (final Throwable t) {
            log.error("Fail to save snapshot, path={}, file list={}, {}.", writerPath, writer.listFiles(),
                StackTraceUtil.stackTrace(t));
            done.run(new Status(RaftError.EIO, "Fail to save snapshot at %s, error is %s", writerPath,
                t.getMessage()));
        }
    }

    public boolean load(final SnapshotReader reader) {
        final LocalFileMeta meta = (LocalFileMeta) reader.getFileMeta(SNAPSHOT_ARCHIVE);
        final String readerPath = reader.getPath();
        if (meta == null) {
            log.error("Can't find kv snapshot file, path={}.", readerPath);
            return false;
        }
        final String snapshotPath = Paths.get(readerPath, SNAPSHOT_DIR).toString();
        try {
            decompressSnapshot(readerPath, meta);
            loadSnapshot(snapshotPath);
            final File tmp = new File(snapshotPath);
            // Delete the decompressed temporary file. If the deletion fails (although it is a small probability
            // event), it may affect the next snapshot decompression result. Therefore, the safest way is to
            // terminate the state machine immediately. Users can choose to manually delete and restart according
            // to the log information.
            if (tmp.exists()) {
                FileUtils.forceDelete(new File(snapshotPath));
            }
            return true;
        } catch (final Throwable t) {
            log.error("Fail to load snapshot, path={}, file list={}, {}.", readerPath, reader.listFiles(),
                StackTraceUtil.stackTrace(t));
            return false;
        }
    }

    private CompletableFuture<LocalFileMeta.Builder> saveSnapshot(String snapshotPath) {
        this.kvStore.writeSnapshot(snapshotPath);
        return CompletableFuture.completedFuture(LocalFileMeta.newBuilder());
    }

    private void loadSnapshot(String snapshotPath) {
        this.kvStore.readSnapshot(snapshotPath);
    }

    private CompletableFuture<LocalFileMeta.Builder> backup(String backupPath) throws IOException {
        ByteString userMeta = ByteString.copyFrom(this.serializer.writeObject(this.kvStore.backupDB(backupPath)));
        return CompletableFuture.completedFuture(LocalFileMeta.newBuilder().setUserMeta(userMeta));
    }

    private void restore(String snapshotPath, LocalFileMeta meta) {
        this.kvStore.restoreBackup(
            snapshotPath,
            this.serializer.readObject(meta.getUserMeta().toByteArray(), RocksDBBackupInfo.class)
        );
    }

    private void compressSnapshot(
        final SnapshotWriter writer,
        final LocalFileMeta.Builder metaBuilder,
        final Closure done
    ) {
        final String writerPath = writer.getPath();
        final String outputFile = Paths.get(writerPath, SNAPSHOT_ARCHIVE).toString();

        try {
            final Checksum checksum = new CRC64();

            ZipStrategyManager.getDefault().compress(writerPath, SNAPSHOT_DIR, outputFile, checksum);
            metaBuilder.setChecksum(Long.toHexString(checksum.getValue()));

            if (writer.addFile(SNAPSHOT_ARCHIVE, metaBuilder.build())) {
                done.run(Status.OK());
            } else {
                done.run(new Status(RaftError.EIO, "Fail to add snapshot file: %s", writerPath));
            }

        } catch (final Throwable t) {
            log.error(
                "Fail to compress snapshot, path={}, file list={}, {}.",
                writerPath,
                writer.listFiles(),
                StackTraceUtil.stackTrace(t)
            );

            done.run(new Status(
                RaftError.EIO,
                "Fail to compress snapshot at %s, error is %s",
                writerPath,
                t.getMessage()
            ));
        }
    }

    private void decompressSnapshot(
        final String readerPath,
        final LocalFileMeta meta
    ) throws Throwable {

        final String sourceFile = Paths.get(readerPath, SNAPSHOT_ARCHIVE).toString();
        final Checksum checksum = new CRC64();

        ZipStrategyManager.getDefault().deCompress(sourceFile, readerPath, checksum);

        if (meta.hasChecksum()) {
            Requires.requireTrue(
                meta.getChecksum().equals(Long.toHexString(checksum.getValue())),
                "Snapshot checksum failed"
            );
        }
    }

}
