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

package io.dingodb.raft.storage.impl;

import io.dingodb.raft.entity.codec.LogEntryDecoder;
import io.dingodb.raft.entity.codec.LogEntryEncoder;
import io.dingodb.raft.option.RaftLogStorageOptions;
import io.dingodb.raft.option.RaftLogStoreOptions;
import io.dingodb.raft.storage.LogStore;
import io.dingodb.raft.util.DebugStatistics;
import io.dingodb.raft.util.Requires;
import io.dingodb.raft.util.StorageOptionsFactory;
import lombok.Getter;
import lombok.Setter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Getter
@Setter
public class RocksDBLogStore implements LogStore<RaftLogStoreOptions> {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBLogStore.class);

    private String path;
    private RocksDB db;
    private DBOptions dbOptions;
    private boolean sync;
    private boolean openStatistics;
    private ColumnFamilyHandle defaultHandle;
    private ColumnFamilyHandle confHandle;
    private WriteOptions writeOptions;
    private ReadOptions totalOrderReadOptions;
    private DebugStatistics statistics;
    private final List<ColumnFamilyOptions> cfOptions = new ArrayList<>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = this.readWriteLock.readLock();
    private final Lock writeLock = this.readWriteLock.writeLock();

    private LogEntryEncoder logEntryEncoder;
    private LogEntryDecoder logEntryDecoder;

    static {
        RocksDB.loadLibrary();
    }

    public static DBOptions createDBOptions() {
        return StorageOptionsFactory.getRocksDBOptions(RocksDBLogStorage.class);
    }

    public static ColumnFamilyOptions createColumnFamilyOptions() {
        final BlockBasedTableConfig tConfig = StorageOptionsFactory
            .getRocksDBTableFormatConfig(RocksDBLogStorage.class);
        return StorageOptionsFactory.getRocksDBColumnFamilyOptions(RocksDBLogStorage.class) //
            .useFixedLengthPrefixExtractor(8) //
            .setTableFormatConfig(tConfig) //
            .setMergeOperator(new StringAppendOperator());
    }

    public boolean init(final RaftLogStoreOptions opts) {
        Requires.requireNonNull(opts.getDataPath(), "Null Log Store DataPath");
        this.path = opts.getDataPath();
        this.sync = opts.isSync();
        this.openStatistics = opts.isOpenStatistics();
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");
        this.writeLock.lock();
        boolean isInputRaftLogEmpty = false;

        try {
            if (this.db != null) {
                LOG.warn("RocksDBLogStorage init() already.");
                return true;
            }
            this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
            this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
            Requires.requireNonNull(this.logEntryDecoder, "Null log entry decoder");
            Requires.requireNonNull(this.logEntryEncoder, "Null log entry encoder");
            this.dbOptions = createDBOptions();
            if (this.openStatistics) {
                this.statistics = new DebugStatistics();
                this.dbOptions.setStatistics(this.statistics);
            }

            this.writeOptions = new WriteOptions();
            this.writeOptions.setSync(this.sync);
            this.totalOrderReadOptions = new ReadOptions();
            this.totalOrderReadOptions.setTotalOrderSeek(true);

            this.dbOptions.setMaxTotalWalSize(4 << 30L);
            RaftLogStorageOptions raftLogStorageOptions = opts.getRaftLogStorageOptions();
            if (raftLogStorageOptions == null) {
                raftLogStorageOptions = new RaftLogStorageOptions();
                isInputRaftLogEmpty = true;
            }

            if (raftLogStorageOptions.getDbMaxTotalWalSize() != 0) {
                this.dbOptions.setMaxTotalWalSize(raftLogStorageOptions.getDbMaxTotalWalSize());
            }

            this.dbOptions.setMaxSubcompactions(4);
            if (raftLogStorageOptions.getDbMaxSubCompactions() != 0) {
                this.dbOptions.setMaxSubcompactions(raftLogStorageOptions.getDbMaxSubCompactions());
            }

            this.dbOptions.setRecycleLogFileNum(4);
            if (raftLogStorageOptions.getDbRecycleLogFileNum() != 0) {
                this.dbOptions.setRecycleLogFileNum(raftLogStorageOptions.getDbRecycleLogFileNum());
            }

            this.dbOptions.setKeepLogFileNum(4);
            if (raftLogStorageOptions.getDbKeepLogFileNum() != 0) {
                this.dbOptions.setKeepLogFileNum(raftLogStorageOptions.getDbKeepLogFileNum());
            }

            this.dbOptions.setDbWriteBufferSize(20 << 30L);
            if (raftLogStorageOptions.getDbWriteBufferSize() != 0) {
                this.dbOptions.setDbWriteBufferSize(raftLogStorageOptions.getDbWriteBufferSize());
            }

            this.dbOptions.setMaxBackgroundJobs(16);
            if (raftLogStorageOptions.getDbMaxBackGroupJobs() != 0) {
                this.dbOptions.setMaxBackgroundJobs(raftLogStorageOptions.getDbMaxBackGroupJobs());
            }

            this.dbOptions.setMaxBackgroundCompactions(8);
            if (raftLogStorageOptions.getDbMaxBackGroupCompactions() != 0) {
                this.dbOptions.setMaxBackgroundCompactions(raftLogStorageOptions.getDbMaxBackGroupCompactions());
            }

            this.dbOptions.setMaxBackgroundFlushes(8);
            if (raftLogStorageOptions.getDbMaxBackGroupFlushes() != 0) {
                this.dbOptions.setMaxBackgroundFlushes(raftLogStorageOptions.getDbMaxBackGroupFlushes());
            }

            this.dbOptions.setMaxManifestFileSize(256 * 1024 * 1024L);
            if (raftLogStorageOptions.getDbMaxManifestFileSize() != 0) {
                this.dbOptions.setMaxManifestFileSize(raftLogStorageOptions.getDbMaxManifestFileSize());
            }
            opts.setRaftLogStorageOptions(raftLogStorageOptions);
            LOG.info("Init raft log dbPath:{}, raft log options:{}, default options:{}",
                this.path,
                raftLogStorageOptions.toString(),
                isInputRaftLogEmpty);
            return initDB(opts);
        } catch (final RocksDBException e) {
            LOG.error("Fail to init RocksDBLogStorage, path={}.", this.path, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }

    }

    private boolean initDB(final RaftLogStoreOptions lopts) throws RocksDBException {
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        final ColumnFamilyOptions cfOption = createColumnFamilyOptions();

        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockSize(128 * 1024);

        RaftLogStorageOptions raftLogStorageOptions = lopts.getRaftLogStorageOptions();
        if (raftLogStorageOptions.getCfBlockSize() != 0) {
            tableConfig.setBlockSize(raftLogStorageOptions.getCfBlockSize());
        }

        tableConfig.setBlockCacheSize(200 / 4  * 1024 * 1024 * 1024L);
        if (raftLogStorageOptions.getCfBlockCacheSize() != 0) {
            tableConfig.setBlockSize(raftLogStorageOptions.getCfBlockCacheSize());
        }
        cfOption.setTableFormatConfig(tableConfig);

        cfOption.setArenaBlockSize(128 * 1024 * 1024);
        if (raftLogStorageOptions.getCfArenaBlockSize() != 0) {
            cfOption.setArenaBlockSize(raftLogStorageOptions.getCfArenaBlockSize());
        }

        cfOption.setMinWriteBufferNumberToMerge(4);
        if (raftLogStorageOptions.getCfMinWriteBufferNumberToMerge() != 0) {
            cfOption.setMinWriteBufferNumberToMerge(raftLogStorageOptions.getCfMinWriteBufferNumberToMerge());
        }

        cfOption.setMaxWriteBufferNumber(5);
        if (raftLogStorageOptions.getCfMaxWriteBufferNumber() != 0) {
            cfOption.setMaxWriteBufferNumber(raftLogStorageOptions.getCfMaxWriteBufferNumber());
        }

        cfOption.setMaxCompactionBytes(512 * 1024 * 1024);
        if (raftLogStorageOptions.getCfMaxCompactionBytes() != 0) {
            cfOption.setMaxCompactionBytes(raftLogStorageOptions.getCfMaxCompactionBytes());
        }

        cfOption.setWriteBufferSize(1 * 1024 * 1024 * 1024);
        if (raftLogStorageOptions.getCfWriteBufferSize() != 0) {
            cfOption.setWriteBufferSize(raftLogStorageOptions.getCfWriteBufferSize());
        }

        this.cfOptions.add(cfOption);
        // Column family to store configuration log entry.
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor("Configuration".getBytes(), cfOption));
        // Default column family to store user data log entry.
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOption));

        openDB(columnFamilyDescriptors);
        return true;
    }

    private void openDB(final List<ColumnFamilyDescriptor> columnFamilyDescriptors) throws RocksDBException {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        final File dir = new File(this.path);
        if (dir.exists() && !dir.isDirectory()) {
            throw new IllegalStateException("Invalid log path, it's a regular file: " + this.path);
        }
        this.db = RocksDB.open(this.dbOptions, this.path, columnFamilyDescriptors, columnFamilyHandles);

        assert (columnFamilyHandles.size() == 2);
        this.confHandle = columnFamilyHandles.get(0);
        this.defaultHandle = columnFamilyHandles.get(1);
    }

    public boolean close() {
        this.writeLock.lock();
        try {
            // The shutdown order is matter.
            // 1. close column family handles
            this.confHandle.close();
            this.defaultHandle.close();
            this.db.close();
            // 2. close column family options.
            for (final ColumnFamilyOptions opt : this.cfOptions) {
                opt.close();
            }
            // 3. close options
            this.dbOptions.close();
            if (this.statistics != null) {
                this.statistics.close();
            }
            this.writeOptions.close();
            this.totalOrderReadOptions.close();
            // 4. help gc.
            this.cfOptions.clear();
            this.dbOptions = null;
            this.statistics = null;
            this.writeOptions = null;
            this.totalOrderReadOptions = null;
            this.defaultHandle = null;
            this.confHandle = null;
            this.db = null;
            LOG.info("LOG DB destroyed, the db path is: {}.", this.path);
        } finally {
            this.writeLock.unlock();
        }
        return true;
    }

    @Override
    public void shutdown() {
        close();
    }
}
