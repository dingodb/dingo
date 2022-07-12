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
import org.rocksdb.ConfigOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.OptionsUtil;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.charset.StandardCharsets.UTF_8;

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

    private static final byte[] CONFIGURATION_COLUMN_FAMILY = "Configuration".getBytes(UTF_8);;

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
            this.dbOptions.setMaxSubcompactions(4);
            this.dbOptions.setRecycleLogFileNum(4);
            this.dbOptions.setKeepLogFileNum(4);
            this.dbOptions.setDbWriteBufferSize(20 << 30L);
            this.dbOptions.setMaxBackgroundJobs(16);
            this.dbOptions.setMaxBackgroundCompactions(8);
            this.dbOptions.setMaxBackgroundFlushes(8);
            this.dbOptions.setMaxManifestFileSize(256 * 1024 * 1024L);
            LOG.info("Init raft log dbPath:{}", this.path);
            return initDB(opts);
        } catch (final RocksDBException e) {
            LOG.error("Fail to init RocksDBLogStorage, path={}.", this.path, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }

    }

    private boolean initDB(final RaftLogStoreOptions opts) throws RocksDBException {
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        final String optionsFile = opts.getLogRocksOptionsFile();
        boolean useDefaultOptions = true;
        try {
            if (optionsFile != null && (new File(optionsFile)).exists()) {
                LOG.info("rocksdb options file found: {}.", optionsFile);
                ConfigOptions configOptions = new ConfigOptions();
                OptionsUtil.loadOptionsFromFile(configOptions, optionsFile, this.dbOptions, columnFamilyDescriptors);
                useDefaultOptions = false;
            } else {
                LOG.info("rocksdb options file not found: {}, use default options.", optionsFile);
            }
        } catch (RocksDBException re) {
            LOG.warn("RocksDBLogStore openDB, load {} exception, use default options.", optionsFile, re);
        }

        if (useDefaultOptions) {
            final ColumnFamilyOptions cfOption = createColumnFamilyOptions();
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
            tableConfig.setBlockSize(128 * 1024);
            tableConfig.setBlockCacheSize(200 / 4 * 1024 * 1024 * 1024L);
            cfOption.setTableFormatConfig(tableConfig);
            cfOption.setArenaBlockSize(128 * 1024 * 1024);
            cfOption.setMinWriteBufferNumberToMerge(4);
            cfOption.setMaxWriteBufferNumber(5);
            cfOption.setMaxCompactionBytes(512 * 1024 * 1024);
            cfOption.setWriteBufferSize(1 * 1024 * 1024 * 1024);

            this.cfOptions.add(cfOption);
            // Column family to store configuration log entry.
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(CONFIGURATION_COLUMN_FAMILY, cfOption));
            // Default column family to store user data log entry.
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOption));
        }

        this.dbOptions.setCreateIfMissing(true);
        openDB(columnFamilyDescriptors, optionsFile);
        return true;
    }

    private void openDB(final List<ColumnFamilyDescriptor> columnFamilyDescriptors, final String optionsFile)
        throws RocksDBException {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        final File dir = new File(this.path);
        if (dir.exists() && !dir.isDirectory()) {
            throw new IllegalStateException("Invalid log path, it's a regular file: " + this.path);
        }

        this.db = RocksDB.open(this.dbOptions, this.path, columnFamilyDescriptors, columnFamilyHandles);
        LOG.info("RocksDBLogStore RocksDB open, path: {}, options file: {}, columnFamilyHandles size: {}.",
            this.path, optionsFile, columnFamilyHandles.size());

        assert (columnFamilyHandles.size() == 2);
        for (int i = 0; i < columnFamilyHandles.size(); i++) {
            if (Arrays.equals(columnFamilyHandles.get(i).getName(), RocksDB.DEFAULT_COLUMN_FAMILY)) {
                this.defaultHandle = columnFamilyHandles.get(i);
            } else if (Arrays.equals(columnFamilyHandles.get(i).getName(), CONFIGURATION_COLUMN_FAMILY)) {
                this.confHandle = columnFamilyHandles.get(i);
            }
        }
        assert (this.confHandle != null);
        assert (this.defaultHandle != null);
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
