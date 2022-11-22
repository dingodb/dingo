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
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompressionType;
import org.rocksdb.ConfigOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FileOperationInfo;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.MemTableInfo;
import org.rocksdb.OptionsUtil;
import org.rocksdb.Range;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;
import org.rocksdb.Status;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.TableFileCreationBriefInfo;
import org.rocksdb.TableFileCreationInfo;
import org.rocksdb.TableFileDeletionInfo;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.WriteStallInfo;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.dingodb.common.codec.PrimitiveCodec.encodeLong;
import static io.dingodb.mpu.Constant.API;
import static io.dingodb.mpu.Constant.CF_DEFAULT;
import static io.dingodb.mpu.Constant.CF_META;
import static io.dingodb.mpu.Constant.CLOCK_K;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.rocksdb.SizeApproximationFlag.INCLUDE_FILES;
import static org.rocksdb.SizeApproximationFlag.INCLUDE_MEMTABLES;

@Slf4j
public class RocksStorage implements Storage {

    static {
        RocksDB.loadLibrary();
    }

    public final CoreMeta coreMeta;

    public final Path path;
    public final Path instructionPath;
    public final Path dbPath;
    public final Path checkpointPath;
    public final Path dcfPath;
    public final Path mcfPath;
    public final Path icfPath;

    public final String dbRocksOptionsFile;
    public final String logRocksOptionsFile;
    public ColumnFamilyConfiguration dcfConf;
    public ColumnFamilyConfiguration mcfConf;
    public ColumnFamilyConfiguration icfConf;
    public final int ttl;

    public final WriteOptions writeOptions;
    public final LinkedRunner runner;

    public Checkpoint checkPoint;
    public RocksDB instruction;
    public RocksDB db;

    private ColumnFamilyHandle dcfHandler;
    private ColumnFamilyHandle mcfHandler;
    private ColumnFamilyHandle icfHandler;

    private ColumnFamilyDescriptor dcfDesc;
    private ColumnFamilyDescriptor mcfDesc;
    private ColumnFamilyDescriptor icfDesc;

    private boolean destroy = false;
    private boolean disableCheckpointPurge = false;

    private static final int MAX_BLOOM_HASH_NUM = 10;
    private static final int MAX_PREFIX_LENGTH = 4;

    public static final String LOCAL_CHECKPOINT_PREFIX = "local-";
    public static final String REMOTE_CHECKPOINT_PREFIX = "remote-";

    public RocksStorage(CoreMeta coreMeta, String path, final String dbRocksOptionsFile,
                        final String logRocksOptionsFile, final int ttl)  throws Exception {
        this.coreMeta = coreMeta;
        this.runner = new LinkedRunner(coreMeta.label);
        this.path = Paths.get(path).toAbsolutePath();
        this.dbRocksOptionsFile = dbRocksOptionsFile;
        this.logRocksOptionsFile = logRocksOptionsFile;
        this.ttl = ttl;

        this.checkpointPath = this.path.resolve("checkpoint");

        this.dbPath = this.path.resolve("db");
        this.dcfPath = this.dbPath.resolve("data");
        this.mcfPath = this.dbPath.resolve("meta");

        RocksConfiguration rocksConfiguration = RocksConfiguration.refreshRocksConfiguration();
        log.info("Loading User Defined RocksConfiguration");
        this.dcfConf = rocksConfiguration.dcfConfiguration();
        this.mcfConf = rocksConfiguration.mcfConfiguration();
        this.icfConf = rocksConfiguration.icfConfiguration();

        this.instructionPath = this.path.resolve("instruction");
        this.icfPath = this.instructionPath.resolve("data");
        FileUtils.createDirectories(this.instructionPath);
        FileUtils.createDirectories(this.checkpointPath);
        FileUtils.createDirectories(this.dbPath);
        this.instruction = createInstruction();
        log.info("Create {} instruction db.", coreMeta.label);
        this.db = createDB();
        this.writeOptions = new WriteOptions();
        log.info("Create {} db,  ttl: {}.", coreMeta.label, this.ttl);
        checkPoint = Checkpoint.create(db);
        log.info("Create rocks storage for {} success.", coreMeta.label);
    }

    private RocksDB createInstruction() throws RocksDBException {
        DBOptions options = new DBOptions();
        loadDBOptions(this.logRocksOptionsFile, options);

        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        options.setListeners(Collections.singletonList(new Listener()));
        options.setWalDir(this.instructionPath.resolve("wal").toString());

        final ColumnFamilyOptions cfOption = new ColumnFamilyOptions();
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockSize(icfConf.getTcBlockSize() == null ? 128 * 1024 :
            Integer.parseInt(icfConf.getTcBlockSize()));

        Cache blockCache = new LRUCache(icfConf.getTcBlockCacheSize() == null ? 200 / 4 * 1024 * 1024 * 1024L :
            Integer.parseInt(icfConf.getTcBlockCacheSize()));
        tableConfig.setBlockCache(blockCache);
        cfOption.setTableFormatConfig(tableConfig);
        cfOption.setArenaBlockSize(icfConf.getCfArenaBlockSize() == null ? 128 * 1024 * 1024 :
            Integer.parseInt(icfConf.getCfArenaBlockSize()));
        cfOption.setMinWriteBufferNumberToMerge(icfConf.getCfMinWriteBufferNumberToMerge() == null ? 4 :
            Integer.parseInt(icfConf.getCfMinWriteBufferNumberToMerge()));
        cfOption.setMaxWriteBufferNumber(icfConf.getCfMaxWriteBufferNumber() == null ? 5 :
            Integer.parseInt(icfConf.getCfMaxWriteBufferNumber()));
        cfOption.setMaxCompactionBytes(icfConf.getCfMaxCompactionBytes() == null ? 512 * 1024 * 1024 :
            Integer.parseInt(icfConf.getCfMaxCompactionBytes()));
        cfOption.setWriteBufferSize(icfConf.getCfWriteBufferSize() == null ? 1024 * 1024 * 1024 :
            Integer.parseInt(icfConf.getCfWriteBufferSize()));
        cfOption.useFixedLengthPrefixExtractor(icfConf.getCfFixedLengthPrefixExtractor() == null ? 8 :
            Integer.parseInt(icfConf.getCfFixedLengthPrefixExtractor()));
        cfOption.setMergeOperator(new StringAppendOperator());

        icfDesc = icfDesc(cfOption);
        List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
        cfs.add(icfDesc);

        List<ColumnFamilyHandle> handles = new ArrayList<>();
        log.info("RocksStorage createInstruction, RocksDB open, path: {}, options file: {}, handles size: {}.",
            this.instructionPath, this.logRocksOptionsFile, handles.size());
        RocksDB instruction = RocksDB.open(options, this.instructionPath.toString(), cfs, handles);
        this.icfHandler = handles.get(0);
        assert (this.icfHandler != null);

        return instruction;
    }

    private RocksDB createDB() throws Exception {
        DBOptions options = new DBOptions();
        loadDBOptions(this.dbRocksOptionsFile, options);
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        options.setWalDir(this.dbPath.resolve("wal").toString());

        /*
         * configuration for performance.
         * 1. max_background_compaction
         * 2. max_background_flushes
         * 3. max_background_jobs
         * 4. bytes_per_sync: 1M
         * 5. db_write_buffer_size: 2G
         */
        options.setListeners(Collections.singletonList(new Listener()));

        List<ColumnFamilyDescriptor> cfs = Arrays.asList(
            dcfDesc = dcfDesc(),
            mcfDesc = mcfDesc()
        );

        RocksDB db;
        List<ColumnFamilyHandle> handles = new ArrayList<>(4);
        if (RocksUtils.ttlValid(this.ttl)) {
            List<Integer> ttlList = new ArrayList<>();
            ttlList.add(this.ttl);
            ttlList.add(0);
            db = TtlDB.open(options, this.dbPath.toString(), cfs, handles, ttlList, false, true);
            Executors.scheduleWithFixedDelayAsync("kv-compact", this::compact,  60 * 60, 60 * 60,
                SECONDS);
        } else {
            db = RocksDB.open(options, this.dbPath.toString(), cfs, handles);
        }
        log.info("RocksStorage createDB, RocksDB open, path: {}, options file: {}, handles size: {}, ttl: {}.",
            this.dbPath, this.dbRocksOptionsFile, handles.size(), this.ttl);
        this.dcfHandler = handles.get(0);
        this.mcfHandler = handles.get(1);
        return db;
    }

    private boolean loadDBOptions(final String optionsFile, DBOptions options) {
        try {
            if (optionsFile == null || !(new File(optionsFile)).exists()) {
                log.info("loadDBOptions, rocksdb options file not found: {}, use default options.", optionsFile);
                return false;
            }
            OptionsUtil.loadDBOptionsSimplyFromFile(new ConfigOptions(), optionsFile, options);
            return true;
        } catch (RocksDBException dbException) {
            log.warn("loadDBOptions, load {} exception, use default options.", optionsFile, dbException);
            return false;
        }
    }

    public void closeDB() {
        this.db.cancelAllBackgroundWork(true);
        this.dcfHandler.close();
        this.dcfHandler = null;
        this.mcfHandler.close();
        this.mcfHandler = null;
        this.db.close();
        this.db = null;
    }

    @Override
    public void destroy() {
        destroy = true;
        this.writeOptions.close();
        closeDB();
        this.icfHandler.close();
        this.icfHandler = null;
        this.instruction.close();
        this.instruction = null;
        this.checkPoint.close();
        this.checkPoint = null;
        /*
         * to avoid the file handle leak when drop table
         */
        // FileUtils.deleteIfExists(path);
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
        log.info(String.format("RocksStorage::transferTo [%s][%s]", meta.label, meta.location.toString()));
        return Executors.submit("transfer-to-" + meta.label, () -> {
            // call backup() to create new checkpoint
            backup();

            //get remote dir to file transfer
            StorageApi storageApi = API.proxy(StorageApi.class, meta.location);
            String target = storageApi.transferBackup(meta.mpuId, meta.coreId);

            //disable checkpoint purge while transferring checkpoint
            this.disableCheckpointPurge = true;
            String checkpointName = getLatestCheckpointName(LOCAL_CHECKPOINT_PREFIX);
            FileTransferService.transferTo(meta.location, checkpointPath.resolve(checkpointName), Paths.get(target));
            this.disableCheckpointPurge = false;

            //call remote node to apply checkpoint into db
            storageApi.applyBackup(meta.mpuId, meta.coreId);
            return null;
        });
    }

    private void flushMeta() {
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {

            if (db != null && mcfHandler != null) {
                db.flush(flushOptions, mcfHandler);
            }
        } catch (RocksDBException e) {
            log.error("Flush instruction error.", e);
        }
    }

    private void flushInstruction() {
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            if (instruction != null) {
                instruction.flush(flushOptions);
            }
        } catch (RocksDBException e) {
            log.error("Flush instruction error.", e);
        }
    }

    /*
     * Create new RocksDB checkpoint in backup dir
     *
     * @throws RuntimeException
     */
    public void createNewCheckpoint() {
        if (destroy) {
            return;
        }
        try {
            if (checkPoint != null) {
                // use nanoTime as checkpointName
                String checkpointName = String.format("%s%d", LOCAL_CHECKPOINT_PREFIX, System.nanoTime());
                String checkpointDirName = this.checkpointPath.resolve(checkpointName).toString();

                log.info("RocksStorage::createNewCheckpoint start " + checkpointName);
                checkPoint.createCheckpoint(checkpointDirName);
                log.info("RocksStorage::createNewCheckpoint finish " + checkpointName);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Get Latest Checkpoint Dir Name
     *
     * @param prefix Prefix of the checkpoint_name
     *              Use prefix to identify remote or local checkpoint.
     */
    public String getLatestCheckpointName(String prefix) {
        String latestCheckpointName = "";
        if (checkPoint != null) {
            File[] directories = new File(this.checkpointPath.toString()).listFiles(File::isDirectory);
            if (directories.length > 0) {
                for (File checkpointDir: directories) {
                    // dir name end with ".tmp" maybe temp dir or litter dir
                    if ((!checkpointDir.getName().endsWith(".tmp")) && checkpointDir.getName().startsWith(prefix)) {
                        if (checkpointDir.getName().compareTo(latestCheckpointName) > 0) {
                            latestCheckpointName = checkpointDir.getName();
                        }
                    }
                }
            }
        }
        return latestCheckpointName;
    }

    /*
     * Purge Old checkpoint, only retain latest [count] checkpoints.
     *
     * @param count Count of checkpoint to retain
     *
     * @throws RuntimeException
     */
    public void purgeOldCheckpoint(int count) {
        if (destroy || disableCheckpointPurge) {
            return;
        }
        try {
            // Sort directory names by alphabetical order
            Comparator<File> comparatorFileName = new Comparator<File>() {
                public int compare(File p1,File p2) {
                    return p2.getName().compareTo(p1.getName());
                }
            };

            File[] directories = new File(this.checkpointPath.toString()).listFiles(File::isDirectory);
            Arrays.sort(directories, comparatorFileName);

            // Delete old checkpoint directories
            int persistCount = 0;
            if (directories.length > count) {
                for (int i = 0; i < directories.length; i++) {
                    // dir name end with ".tmp" is delayed to delete
                    if (!directories[i].getName().endsWith(".tmp")) {
                        persistCount++;
                    }

                    if (persistCount > count) {
                        log.info("RocksStorage::purgeOldCheckpoint delete " + directories[i].toString());
                        FileUtils.deleteIfExists(directories[i].toPath());
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Restore db from latest checkpoint
     *
     * @throws RuntimeException
     */
    @Override
    public void applyBackup() {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            String remoteCheckpointDir = String.format("%s%s", REMOTE_CHECKPOINT_PREFIX, "checkpoint");
            if (remoteCheckpointDir.length() == 0) {
                throw new RuntimeException("GetLatestCheckpointName return null string");
            }
            log.info("RocksStorage::applyBackup start [" + remoteCheckpointDir + "]");

            //1.generate temp new db dir for new RocksDB
            Path tempNewDbPath = this.path.resolve("load_from_" + remoteCheckpointDir);

            //2.rename remote checkpoint to tempNewDbPath
            Files.move(this.path.resolve(remoteCheckpointDir), tempNewDbPath);

            //3.rename old db to will_delete_soon_[checkpoint_name]
            checkPoint.close();
            checkPoint = null;
            closeDB();

            Path tempOldDbPath = this.path.resolve("will_delete_soon_" + remoteCheckpointDir);
            Files.move(this.dbPath, tempOldDbPath);

            //4.rename temp new db dir to new db dir
            Files.move(tempNewDbPath, this.dbPath);

            //5.createDB()
            db = createDB();
            checkPoint = Checkpoint.create(db);

            //6.delete old db thoroughly
            FileUtils.deleteIfExists(tempOldDbPath);
            log.info("RocksStorage::applyBackup finished [" + remoteCheckpointDir + "]");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void backup() {
        if (destroy) {
            return;
        }

        try {
            createNewCheckpoint();
            purgeOldCheckpoint(3);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String receiveBackup() {
        String checkpointName = String.format("%s%s", REMOTE_CHECKPOINT_PREFIX, "checkpoint");
        log.info(String.format("receiveBackup path=[%s]\n", this.path.resolve(checkpointName).toString()));

        FileUtils.deleteIfExists(this.path.resolve(checkpointName));
        FileUtils.createDirectories(this.path.resolve(checkpointName));

        return this.path.resolve(checkpointName).toString();
    }

    @Override
    public long approximateCount() {
        try {
            if (destroy) {
                throw new RuntimeException();
            }
            return db.getLongProperty(dcfHandler, "rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long approximateSize() {
        if (destroy) {
            throw new RuntimeException();
        }
        try (
            Snapshot snapshot = db.getSnapshot();
            ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot)
        ) {
            try (final RocksIterator it = this.db.newIterator(readOptions)) {
                Slice start = null;
                Slice limit = null;
                it.seekToFirst();
                if (it.isValid()) {
                    start = new Slice(it.key());
                }
                it.seekToLast();
                if (it.isValid()) {
                    limit = new Slice(it.key());
                }
                if (start != null && limit != null) {
                    return Arrays.stream(
                        db.getApproximateSizes(singletonList(new Range(start, limit)), INCLUDE_FILES, INCLUDE_MEMTABLES)
                    ).sum();
                }
            } finally {
                readOptions.setSnapshot(null);
                db.releaseSnapshot(snapshot);
            }
        }
        return 0;
    }

    @Override
    public void clearClock(long clock) {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            instruction.delete(icfHandler, encodeLong(clock));
            if (clock % 1000000 == 0) {
                instruction.deleteRange(icfHandler, encodeLong(0), encodeLong(clock));
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long clocked() {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            return Optional.mapOrGet(db.get(mcfHandler, CLOCK_K), PrimitiveCodec::readLong, () -> 0L);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long clock() {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            return Optional.mapOrGet(instruction.get(icfHandler, CLOCK_K), PrimitiveCodec::readLong, () -> 0L);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void tick(long clock) {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            this.instruction.put(icfHandler, CLOCK_K, PrimitiveCodec.encodeLong(clock));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveInstruction(long clock, byte[] instruction) {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            this.instruction.put(icfHandler, PrimitiveCodec.encodeLong(clock), instruction);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] reappearInstruction(long clock) {
        if (destroy) {
            throw new RuntimeException();
        }
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
        if (destroy) {
            throw new RuntimeException();
        }
        return new Reader(db, dcfHandler);
    }

    @Override
    public Writer writer(Instruction instruction) {
        if (destroy) {
            throw new RuntimeException();
        }
        return new Writer(db, instruction, dcfHandler);
    }

    @Override
    public void flush(io.dingodb.mpu.storage.Writer writer) {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            Instruction instruction = writer.instruction();
            WriteBatch batch = ((Writer) writer).writeBatch();
            byte[] clockValue = PrimitiveCodec.encodeLong(instruction.clock);
            if (RocksUtils.ttlValid(this.ttl)) {
                clockValue = RocksUtils.getValueWithNowTs(clockValue);
            }
            batch.put(mcfHandler, CLOCK_K, clockValue);
            this.db.write(writeOptions, batch);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                writer.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void compact() {
        long now = System.currentTimeMillis();
        try {
            if (this.db != null) {
                this.db.compactRange();
            } else {
                log.info("RocksStorage compact db is null.");
            }
        } catch (final Exception e) {
            log.error("RocksStorage compact exception, label: {}.", this.coreMeta.label, e);
            throw new RuntimeException(e);
        }
        log.info("RocksStorage compact, label: {}, cost {}s.", this.coreMeta.label,
            (System.currentTimeMillis() - now) / 1000 );
    }

    private ColumnFamilyDescriptor dcfDesc() {
        final ColumnFamilyOptions cfOption = new ColumnFamilyOptions();
        /*
         * configuration for performance.
         * write_buffer_size: will control the sst file size
         */
        cfOption.setMaxWriteBufferNumber(dcfConf.getCfMaxWriteBufferNumber() == null ? 5 :
            Integer.parseInt(dcfConf.getCfMaxWriteBufferNumber()));
        cfOption.setWriteBufferSize(dcfConf.getCfWriteBufferSize() == null ? 256L * 1024 * 1024 :
            Integer.parseInt(dcfConf.getCfWriteBufferSize()));
        cfOption.setMaxBytesForLevelBase(dcfConf.getCfMaxBytesForLevelBase() == null ? 1024 * 1024 * 1024L :
            Integer.parseInt(dcfConf.getCfMaxBytesForLevelBase()));
        cfOption.setTargetFileSizeBase(dcfConf.getCfTargetFileSizeBase() == null ? 64L * 1024 * 1024 :
            Integer.parseInt(dcfConf.getCfTargetFileSizeBase()));
        cfOption.setMinWriteBufferNumberToMerge(dcfConf.getCfMinWriteBufferNumberToMerge() == null ? 1 :
            Integer.parseInt(dcfConf.getCfMinWriteBufferNumberToMerge()));

        List<CompressionType> compressionTypes = Arrays.asList(
            CompressionType.NO_COMPRESSION,
            CompressionType.NO_COMPRESSION,
            CompressionType.LZ4_COMPRESSION,
            CompressionType.LZ4_COMPRESSION,
            CompressionType.LZ4_COMPRESSION,
            CompressionType.ZSTD_COMPRESSION,
            CompressionType.ZSTD_COMPRESSION);
        cfOption.setCompressionPerLevel(compressionTypes);

        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockSize(dcfConf.getTcBlockSize() == null ? 128L * 1024 :
            Integer.parseInt(dcfConf.getTcBlockSize()));

        tableConfig.setFilterPolicy(new BloomFilter(MAX_BLOOM_HASH_NUM, false));
        tableConfig.setWholeKeyFiltering(true);
        // tableConfig.setCacheIndexAndFilterBlocks(true);
        cfOption.useCappedPrefixExtractor(MAX_PREFIX_LENGTH);

        Cache blockCache = new LRUCache(dcfConf.getTcBlockCacheSize() == null ?  1024 * 1024 * 1024 :
            Integer.parseInt(dcfConf.getTcBlockCacheSize()));
        tableConfig.setBlockCache(blockCache);
        Cache compressedBlockCache = new LRUCache(dcfConf.getTcBlockCacheCompressedSize() == null
            ? 1024 * 1024 * 1024 : Integer.parseInt(dcfConf.getTcBlockCacheCompressedSize()));
        tableConfig.setBlockCacheCompressed(compressedBlockCache);
        cfOption.setTableFormatConfig(tableConfig);
        return new ColumnFamilyDescriptor(CF_DEFAULT, cfOption);
    }

    private static ColumnFamilyDescriptor icfDesc() {
        return new ColumnFamilyDescriptor(CF_DEFAULT, new ColumnFamilyOptions());
    }

    private static ColumnFamilyDescriptor icfDesc(ColumnFamilyOptions cfOptions) {
        return new ColumnFamilyDescriptor(CF_DEFAULT, cfOptions);
    }

    private static ColumnFamilyDescriptor mcfDesc() {
        return new ColumnFamilyDescriptor(CF_META, new ColumnFamilyOptions());
    }

    public class Listener extends AbstractEventListener {

        @Override
        public void onFlushCompleted(RocksDB db, FlushJobInfo flushJobInfo) {
            log.info("{} on flush completed, info: {}", coreMeta.label, flushJobInfo);
        }

        @Override
        public void onFlushBegin(RocksDB db, FlushJobInfo flushJobInfo) {
            log.info("{} on flush begin, info: {}", coreMeta.label, flushJobInfo);
        }

        @Override
        public void onTableFileDeleted(TableFileDeletionInfo tableFileDeletionInfo) {
            log.info("{} on table file deleted, info: {}", coreMeta.label, tableFileDeletionInfo);
        }

        @Override
        public void onCompactionBegin(RocksDB db, CompactionJobInfo compactionJobInfo) {
            log.info("{} on compaction begin, info: {}", coreMeta.label, compactionJobInfo);
        }

        @Override
        public void onCompactionCompleted(RocksDB db, CompactionJobInfo compactionJobInfo) {
            log.info("{} on compaction completed, info: {}", coreMeta.label, compactionJobInfo);
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
        }

        @Override
        public void onFileCloseFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file close finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public boolean onErrorRecoveryBegin(BackgroundErrorReason reason, Status status) {
            log.info(
                "{} on error recovery begin, reason: {}, code: {}, state: {}",
                coreMeta.label, reason, status.getCodeString(), status.getState()
            );
            return super.onErrorRecoveryBegin(reason, status);
        }

        @Override
        public void onErrorRecoveryCompleted(Status status) {
            log.info(
                "{} on error recovery completed, code: {}, state: {}",
                coreMeta.label, status.getCodeString(), status.getState()
            );
        }

    }
}

