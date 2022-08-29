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

package io.dingodb.mpu.storage.rocks;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.util.FileUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.mpu.api.StorageApi;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.instruction.Instruction;
import io.dingodb.mpu.storage.Storage;
import io.dingodb.net.service.FileTransferService;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.BackgroundErrorReason;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupEngineOptions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.DBOptions;
import org.rocksdb.FileOperationInfo;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.MemTableInfo;
import org.rocksdb.RestoreOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.rocksdb.TableFileCreationBriefInfo;
import org.rocksdb.TableFileCreationInfo;
import org.rocksdb.TableFileDeletionInfo;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.WriteStallInfo;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.dingodb.common.codec.PrimitiveCodec.encodeLong;
import static io.dingodb.mpu.Constant.API;
import static io.dingodb.mpu.Constant.CF_DEFAULT;
import static io.dingodb.mpu.Constant.CF_META;
import static io.dingodb.mpu.Constant.CLOCK_K;

@Slf4j
public class RocksStorage implements Storage {

    public final CoreMeta coreMeta;

    public final Path path, instructionPath, dbPath, backupPath, dcfPath, mcfPath, icfPath;

    public final DBOptions options;
    public final BackupEngine backup;
    public final LinkedRunner runner;
    public final RocksDB instruction;
    public final RocksDB db;


    private ColumnFamilyHandle dcfHandler, mcfHandler, icfHandler;
    private ColumnFamilyDescriptor dcfDesc, mcfDesc, icfDesc;

    public RocksStorage(CoreMeta coreMeta, String path, DBOptions options) throws Exception {
        this.coreMeta = coreMeta;
        this.runner = new LinkedRunner(coreMeta.label);
        this.path = Paths.get(path).toAbsolutePath();

        this.backupPath = this.path.resolve("backup");

        this.dbPath = this.path.resolve("db");
        this.dcfPath = this.dbPath.resolve("data");
        this.mcfPath = this.dbPath.resolve("meta");

        this.instructionPath = this.path.resolve("instruction");
        this.icfPath = this.instructionPath.resolve("data");
        this.options = options;
        FileUtils.createDirectories(this.instructionPath);
        FileUtils.createDirectories(this.backupPath);
        FileUtils.createDirectories(this.dbPath);
        this.instruction = createInstruction();
        this.db = createDB(options);
        backup = BackupEngine.open(db.getEnv(), new BackupEngineOptions(backupPath.toString()));
    }

    private RocksDB createInstruction() throws RocksDBException {
        DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        options.setWalDir(this.instructionPath.resolve("wal").toString());
        options.setDbLogDir(this.instructionPath.resolve("log").toString());
        List<ColumnFamilyDescriptor> cfs = Arrays.asList(
            icfDesc = icfDesc(icfPath)
        );
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        RocksDB instruction = RocksDB.open(options, instructionPath.toString(), cfs, handles);
        icfHandler = handles.get(0);
        return instruction;
    }

    private RocksDB createDB(DBOptions options) throws Exception {
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        options.setWalDir(this.dbPath.resolve("wal").toString());
        options.setDbLogDir(this.dbPath.resolve("log").toString());
        options.setListeners(Collections.singletonList(new Listener()));
        List<ColumnFamilyDescriptor> cfs = Arrays.asList(
            dcfDesc = dcfDesc(dcfPath),
            mcfDesc = mcfDesc(mcfPath)
        );
        List<ColumnFamilyHandle> handles = new ArrayList<>(4);
        RocksDB db = RocksDB.open(options, dbPath.toString(), cfs, handles);
        this.dcfHandler = handles.get(0);
        this.mcfHandler = handles.get(1);
        return db;
    }

    public void closeDB() {
        this.db.close();
        this.dcfHandler.close();
        this.mcfHandler.close();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        try {
            dcfDesc.getOptions().close();
            mcfDesc.getOptions().close();
            icfDesc.getOptions().close();
        } catch (Exception e) {
            log.error("Close {} cf options error.", coreMeta.label, e);
        }
    }

    @Override
    public CompletableFuture<Void> transferTo(CoreMeta meta) {
        return Executors.submit("transfer-to-" + meta.label, () -> {
            backup.createNewBackup(db);
            backup.purgeOldBackups(3);
            StorageApi storageApi = API.proxy(StorageApi.class, meta.location);
            String target = storageApi.transferBackup(meta.mpuId, meta.coreId);
            FileTransferService.transferTo(meta.location, Paths.get(backupPath.toString()), Paths.get(target));
            storageApi.applyBackup(meta.mpuId, meta.coreId);
            return null;
        });
    }

    private void backup() {
        try {
            long clock = clocked();
            backup.createNewBackup(db);
            backup.purgeOldBackups(3);
            //instruction.deleteRange(PrimitiveCodec.encodeLong(0), PrimitiveCodec.encodeLong(clock));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String receiveBackup() {
        return this.backupPath.toString();
    }

    @Override
    public void applyBackup() {
        try {
            db.close();
            backup.restoreDbFromLatestBackup(
                dbPath.toString(), dbPath.resolve("wal").toString(), new RestoreOptions(false)
            );
            createDB(options);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clearClock(long clock) {
        try {
            instruction.deleteRange(icfHandler, encodeLong(clock), encodeLong(clock + 1));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long clocked() {
        try {
            return Optional.mapOrGet(db.get(mcfHandler, CLOCK_K), PrimitiveCodec::readLong, () -> 0L);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long clock() {
        try {
            return Optional.mapOrGet(instruction.get(icfHandler, CLOCK_K), PrimitiveCodec::readLong, () -> 0L);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void tick(long clock) {
        try {
            this.instruction.put(icfHandler, CLOCK_K, PrimitiveCodec.encodeLong(clock));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveInstruction(long clock, byte[] instruction) {
        try {
            this.instruction.put(icfHandler, PrimitiveCodec.encodeLong(clock), instruction);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] reappearInstruction(long clock) {
        try {
            return instruction.get(icfHandler, PrimitiveCodec.encodeLong(clock));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String filePath() {
        return null;
    }

    @Override
    public Reader reader() {
        return new Reader(db, dcfHandler);
    }

    @Override
    public Writer writer(Instruction instruction) {
        return new Writer(db, instruction, dcfHandler);
    }

    @Override
    public void flush(io.dingodb.mpu.storage.Writer writer) {
        try {
            Instruction instruction = writer.instruction();
            WriteBatch batch = ((Writer) writer).writeBatch();
            batch.put(mcfHandler, CLOCK_K, PrimitiveCodec.encodeLong(instruction.clock));
            this.db.write(new WriteOptions(), batch);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static ColumnFamilyDescriptor dcfDesc(Path dcfPath) {
        return new ColumnFamilyDescriptor(CF_DEFAULT, new ColumnFamilyOptions()
            //.setCfPaths(Collections.singleton(new DbPath(dcfPath, 0)))
            .setLevel0SlowdownWritesTrigger(-1)
            .setLevel0StopWritesTrigger(-1)
        );
    }

    private static ColumnFamilyDescriptor icfDesc(Path icfPath) {
        return new ColumnFamilyDescriptor(CF_DEFAULT, new ColumnFamilyOptions()
            //.setCfPaths(Collections.singleton(new DbPath(icfPath, 0)))
            .setNumLevels(2)
            .setLevel0FileNumCompactionTrigger(2)
        );
    }


    private static ColumnFamilyDescriptor mcfDesc(Path mcfPath) {
        return new ColumnFamilyDescriptor(CF_META, new ColumnFamilyOptions()
            //.setCfPaths(Collections.singleton(new DbPath(mcfPath, 0)))
            .setNumLevels(2)
            .setLevel0FileNumCompactionTrigger(2)
        );
    }

    public class Listener extends AbstractEventListener {

        @Override
        public void onFlushCompleted(RocksDB db, FlushJobInfo flushJobInfo) {
            log.info("{} on flush completed, info: {}", coreMeta.label, flushJobInfo);
            runner.forceFollow(RocksStorage.this::backup);
        }

        @Override
        public void onFlushBegin(RocksDB db, FlushJobInfo flushJobInfo) {
            log.info("{} on flush begin, info: {}", coreMeta.label, flushJobInfo);
        }

        @Override
        public void onTableFileDeleted(TableFileDeletionInfo tableFileDeletionInfo) {
            log.info("{} on table file deleted, info: {}", coreMeta.label, tableFileDeletionInfo);
            runner.forceFollow(RocksStorage.this::backup);

        }

        @Override
        public void onCompactionBegin(RocksDB db, CompactionJobInfo compactionJobInfo) {
            log.info("{} on compaction begin, info: {}", coreMeta.label, compactionJobInfo);
        }

        @Override
        public void onCompactionCompleted(RocksDB db, CompactionJobInfo compactionJobInfo) {
            log.info("{} on compaction completed, info: {}", coreMeta.label, compactionJobInfo);
            runner.forceFollow(RocksStorage.this::backup);

        }

        @Override
        public void onTableFileCreated(TableFileCreationInfo tableFileCreationInfo) {
            log.info("{} on table file created, info: {}", coreMeta.label, tableFileCreationInfo);
        }

        @Override
        public void onTableFileCreationStarted(TableFileCreationBriefInfo tableFileCreationBriefInfo) {
            log.info("{} on table file creation started, info: {}", coreMeta.label, tableFileCreationBriefInfo);
        }

        @Override
        public void onMemTableSealed(MemTableInfo memTableInfo) {
            log.info("{} on mem table sealed, info: {}", coreMeta.label, memTableInfo);
        }

        @Override
        public void onBackgroundError(BackgroundErrorReason reason, Status status) {
            log.error(
                "{} on background error, reason: {}, code: {}, state: {}",
                coreMeta.label, reason, status.getCodeString(), status.getState()
            );
        }

        @Override
        public void onStallConditionsChanged(WriteStallInfo writeStallInfo) {
            log.info("{} on stall conditions changed, info: {}", coreMeta.label, writeStallInfo);
        }

        @Override
        public void onFileReadFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file read finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public void onFileWriteFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file write finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public void onFileFlushFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file flush finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public void onFileSyncFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file sync finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public void onFileRangeSyncFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file range sync finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public void onFileTruncateFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file truncate finish, info: {}", coreMeta.label, fileOperationInfo);
            runner.forceFollow(RocksStorage.this::backup);
        }

        @Override
        public void onFileCloseFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file close finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public boolean onErrorRecoveryBegin(BackgroundErrorReason reason, Status status) {
            log.error(
                "{} on error recovery begin, reason: {}, code: {}, state: {}", coreMeta.label, reason, status.getCodeString(), status.getState()
            );
            return super.onErrorRecoveryBegin(reason, status);
        }

        @Override
        public void onErrorRecoveryCompleted(Status status) {
            log.info(
                "{} on error recovery completed, code: {}, state: {}", coreMeta.label, status.getCodeString(), status.getState()
            );
        }

    }


}
