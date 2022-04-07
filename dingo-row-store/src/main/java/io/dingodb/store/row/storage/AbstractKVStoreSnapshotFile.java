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

import com.google.protobuf.ByteString;
import io.dingodb.raft.rpc.ReportTarget;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.raft.Closure;
import io.dingodb.raft.Status;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.storage.snapshot.SnapshotReader;
import io.dingodb.raft.storage.snapshot.SnapshotWriter;
import io.dingodb.raft.util.CRC64;
import io.dingodb.raft.util.Requires;
import io.dingodb.store.row.serialization.Serializer;
import io.dingodb.store.row.serialization.Serializers;
import io.dingodb.store.row.storage.zip.ZipStrategyManager;
import io.dingodb.store.row.util.StackTraceUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.zip.Checksum;

import static io.dingodb.raft.entity.LocalFileMetaOutter.LocalFileMeta;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class AbstractKVStoreSnapshotFile implements KVStoreSnapshotFile {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractKVStoreSnapshotFile.class);

    private static final String SNAPSHOT_DIR = "kv";
    private static final String SNAPSHOT_ARCHIVE = "kv.zip";

    protected final Serializer serializer = Serializers.getDefault();

    @Override
    public void save(final SnapshotWriter writer, final Region region, final Closure done,
                     final ExecutorService executor, final ReportTarget reportTarget) {
        final String writerPath = writer.getPath();
        final String snapshotPath = Paths.get(writerPath, SNAPSHOT_DIR).toString();
        try {
            doSnapshotSave(snapshotPath, region, executor, reportTarget).whenComplete((metaBuilder, throwable) -> {
                if (throwable == null) {
                    executor.execute(() -> compressSnapshot(writer, metaBuilder, done));
                } else {
                    LOG.error("Fail to save snapshot, path={}, file list={}, {}.", writerPath, writer.listFiles(),
                            StackTraceUtil.stackTrace(throwable));
                    done.run(new Status(RaftError.EIO, "Fail to save snapshot at %s, error is %s", writerPath,
                            throwable.getMessage()));
                }
            });
        } catch (final Throwable t) {
            LOG.error("Fail to save snapshot, path={}, file list={}, {}.", writerPath, writer.listFiles(),
                    StackTraceUtil.stackTrace(t));
            done.run(new Status(RaftError.EIO, "Fail to save snapshot at %s, error is %s", writerPath,
                    t.getMessage()));
        }
    }

    @Override
    public boolean load(final SnapshotReader reader, final Region region) {
        final LocalFileMeta meta = (LocalFileMeta) reader.getFileMeta(SNAPSHOT_ARCHIVE);
        final String readerPath = reader.getPath();
        if (meta == null) {
            LOG.error("Can't find kv snapshot file, path={}.", readerPath);
            return false;
        }
        final String snapshotPath = Paths.get(readerPath, SNAPSHOT_DIR).toString();
        try {
            decompressSnapshot(readerPath, meta);
            doSnapshotLoad(snapshotPath, meta, region);
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
            LOG.error("Fail to load snapshot, path={}, file list={}, {}.", readerPath, reader.listFiles(),
                StackTraceUtil.stackTrace(t));
            return false;
        }
    }

    abstract CompletableFuture<LocalFileMeta.Builder> doSnapshotSave(final String snapshotPath,
                                                                     final Region region,
                                                                     final ExecutorService executor,
                                                                     final ReportTarget reportTarget) throws Exception;

    abstract void doSnapshotLoad(final String snapshotPath, final LocalFileMeta meta, final Region region)
                                                                                                    throws Exception;

    protected void compressSnapshot(final SnapshotWriter writer, final LocalFileMeta.Builder metaBuilder,
                                    final Closure done) {
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
            LOG.error("Fail to compress snapshot, path={}, file list={}, {}.", writerPath, writer.listFiles(),
                StackTraceUtil.stackTrace(t));
            done.run(new Status(RaftError.EIO, "Fail to compress snapshot at %s, error is %s", writerPath, t
                .getMessage()));
        }
    }

    protected void decompressSnapshot(final String readerPath, final LocalFileMeta meta) throws Throwable {
        final String sourceFile = Paths.get(readerPath, SNAPSHOT_ARCHIVE).toString();
        final Checksum checksum = new CRC64();
        ZipStrategyManager.getDefault().deCompress(sourceFile, readerPath, checksum);
        if (meta.hasChecksum()) {
            Requires.requireTrue(meta.getChecksum().equals(Long.toHexString(checksum.getValue())),
                "Snapshot checksum failed");
        }
    }

    protected <T> T readMetadata(final LocalFileMeta meta, final Class<T> cls) {
        final ByteString userMeta = meta.getUserMeta();
        return this.serializer.readObject(userMeta.toByteArray(), cls);
    }

    protected <T> LocalFileMeta.Builder writeMetadata(final T metadata) {
        if (metadata == null) {
            return LocalFileMeta.newBuilder();
        }
        return LocalFileMeta.newBuilder().setUserMeta(ByteString.copyFrom(this.serializer.writeObject(metadata)));
    }
}
