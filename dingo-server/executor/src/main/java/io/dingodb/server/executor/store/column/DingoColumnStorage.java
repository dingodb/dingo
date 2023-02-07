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

package io.dingodb.server.executor.store.column;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.FileUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.instruction.Instruction;
import io.dingodb.mpu.storage.Storage;
import io.dingodb.mpu.storage.rocks.ColumnFamilyConfiguration;
import io.dingodb.mpu.storage.rocks.RocksConfiguration;
import io.dingodb.strorage.column.ColumnBlock;
import io.dingodb.strorage.column.ColumnStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.StringAppendOperator;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static io.dingodb.common.codec.PrimitiveCodec.encodeLong;
import static io.dingodb.mpu.Constant.CF_DEFAULT;
import static io.dingodb.mpu.Constant.CLOCK_K;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class DingoColumnStorage implements Storage {
    public final String label;

    public final Path path;
    public final Path instructionPath;
    public final Path dbPath;
    private final String database = "default";
    private final String table;
    private final ColumnStorage columnStorage;

    public ColumnFamilyConfiguration icfConf;
    public final LinkedRunner runner;
    public Checkpoint checkPoint;
    public RocksDB instruction;
    public RocksDB db;

    private ColumnFamilyHandle mcfHandler;
    private ColumnFamilyHandle icfHandler;

    private ColumnFamilyDescriptor mcfDesc;
    private ColumnFamilyDescriptor icfDesc;
    @Getter
    private boolean destroy = false;
    private DBOptions instructionDBOptions;
    private DBOptions metaDBOptions;
    private TableDefinition definition;

    public DingoColumnStorage(String label, Path path, final String table, TableDefinition definition) throws Exception {
        this.label = label;
        this.runner = new LinkedRunner(label);
        this.path = path.toAbsolutePath();

        RocksConfiguration rocksConfiguration = RocksConfiguration.refreshRocksConfiguration();
        this.dbPath = this.path.resolve("column_db");

        this.icfConf = rocksConfiguration.icfConfiguration();

        this.instructionPath = this.path.resolve("column_instruction");
        FileUtils.createDirectories(this.instructionPath);
        FileUtils.createDirectories(this.dbPath);

        this.instruction = createInstruction();
        log.info("Create {} instruction db.", label);

        this.db = createDB();
        log.info("Create {} db.", label);

        Executors.scheduleWithFixedDelayAsync("column-merge", this::merge, 5 * 60, 5 * 60, SECONDS);
        this.columnStorage =  new ColumnStorage();
        this.table = table;
        log.info("DingoColumnStorage, table: {}.", this.table);
        this.definition = definition;
    }

    private RocksDB createInstruction() throws RocksDBException {
        this.instructionDBOptions = new DBOptions();
        this.instructionDBOptions.setCreateIfMissing(true);
        this.instructionDBOptions.setCreateMissingColumnFamilies(true);
        this.instructionDBOptions.setWalDir(this.instructionPath.resolve("wal").toString());

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
        log.info("DingoColumnStorage createInstruction, RocksDB open, path: {}, handles size: {}.",
            this.instructionPath, handles.size());
        RocksDB instruction = RocksDB.open(this.instructionDBOptions, this.instructionPath.toString(), cfs, handles);
        this.icfHandler = handles.get(0);
        assert (this.icfHandler != null);

        return instruction;
    }

    private RocksDB createDB() throws Exception {
        metaDBOptions = new DBOptions();
        metaDBOptions.setCreateIfMissing(true);
        metaDBOptions.setCreateMissingColumnFamilies(true);
        metaDBOptions.setWalDir(this.dbPath.resolve("wal").toString());

        List<ColumnFamilyDescriptor> cfs = Arrays.asList(
            mcfDesc = mcfDesc()
        );

        List<ColumnFamilyHandle> handles = new ArrayList<>(4);
        RocksDB db = RocksDB.open(metaDBOptions, this.dbPath.toString(), cfs, handles);
        log.info("DingoColumnStorage createDB, RocksDB open, path: {}, handles size: {}.",
            this.dbPath, handles.size());
        this.mcfHandler = handles.get(0);
        return db;
    }

    public void closeDB() {
        if (this.db != null) {
            this.db.cancelAllBackgroundWork(true);
        }
        if (this.mcfHandler != null) {
            this.mcfHandler.close();
            this.mcfHandler = null;
        }
        if (this.db != null) {
            this.db.close();
            this.db = null;
        }

        if (this.instructionDBOptions != null) {
            this.instructionDBOptions.close();
            this.instructionDBOptions = null;
        }
        if (this.metaDBOptions != null) {
            this.metaDBOptions.close();
            this.metaDBOptions = null;
        }
    }

    @Override
    public long clocked() {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            return Optional.mapOrGet(db.get(mcfHandler, CLOCK_K), PrimitiveCodec::decodeLong, () -> 0L);
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
            return Optional.mapOrGet(instruction.get(icfHandler, CLOCK_K), PrimitiveCodec::decodeLong, () -> 0L);
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
    public synchronized void destroy() {
        if (destroy) {
            return;
        }
        destroy = true;
        closeDB();
        if (this.icfHandler != null) {
            this.icfHandler.close();
            this.icfHandler = null;
        }
        if (this.instruction != null) {
            this.instruction.close();
            this.instruction = null;
        }
        if (this.checkPoint != null) {
            this.checkPoint.close();
            this.checkPoint = null;
        }

        try {
            if (log.isDebugEnabled()) {
                log.debug("DingoColumnStorage destroy instruction db path {}", this.instructionPath.toAbsolutePath());
            }
            Options options = new Options();
            options.setWalDir(this.dbPath.resolve("wal").toString());
            RocksDB.destroyDB(this.dbPath.toAbsolutePath().toString(), options);
            options.setWalDir(this.instructionPath.resolve("wal").toString());
            RocksDB.destroyDB(this.instructionPath.toAbsolutePath().toString(), options);

            if (this.path != null) {
                FileUtils.deleteIfExists(this.path);
            }
        } catch (RocksDBException e) {
            log.error("DingoColumnStorage destroy db failed", e);
        }
        this.dropTable();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        try {
            mcfDesc.getOptions().close();
            icfDesc.getOptions().close();
        } catch (Exception e) {
            log.error("DingoColumnStorage Close {} cf options error.", label, e);
        }
    }

    @Override
    public CompletableFuture<Void> transferTo(CoreMeta meta) {
        log.info(String.format("DingoColumnStorage::transferTo [%s][%s]", meta.label, meta.location.toString()));
        return null;
    }

    @Override
    public String filePath() {
        return null;
    }

    @Override
    public String receiveBackup() {
        return null;
    }

    @Override
    public void applyBackup() {
        return;
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
    public long approximateCount() {
        return 0;
    }

    @Override
    public long approximateSize() {
        return 0;
    }

    @Override
    public Reader metaReader() {
        throw new RuntimeException("Unsupported");
    }

    @Override
    public Writer metaWriter(Instruction instruction) {
        throw new RuntimeException("Unsupported");
    }

    @Override
    public Reader reader() {
        return new Reader(definition, table);
    }

    @Override
    public Writer writer(Instruction instruction) {
        return new Writer(instruction, definition, this.reader());
    }

    @Override
    public void flush(io.dingodb.mpu.storage.Writer storageWriter) {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            Writer writer;
            if (storageWriter instanceof Writer) {
                writer = (Writer) storageWriter;
            } else {
                throw new RuntimeException("write error!");
            }
            Instruction instruction = writer.instruction();
            byte[] clockValue = PrimitiveCodec.encodeLong(instruction.clock);
            this.db.put(mcfHandler, CLOCK_K, clockValue);
            String[] columnNames = writer.getColumnNames();
            String[][] columnData = writer.getColumnData();
            if (columnData == null) {
                log.warn("column data is null while flush");
                return;
            }
            if (columnData.length > 0) {
                log.info("DingoColumnStorage column count: {}, row count: {}.", columnData.length, columnData[0].length);
            } else {
                log.info("DingoColumnStorage column count: {}, row count: 0, no rows.", columnData.length);
            }
            if (columnNames.length != columnData.length) {
                throw new RuntimeException("column data length not match: " + columnNames.length + " | " +
                    columnData.length);
            }

            int length = 0;
            for (int i = 0; i < columnData.length; i++) {
                if (length == 0) {
                    length = columnData[i].length;
                } else {
                    if (length != columnData[i].length) {
                        throw new RuntimeException("row data size not match: " + i + " | " + length + " | " +
                            columnData[i].length);
                    }
                }
            }

            log.info("DingoColumnStorage flush!");
            try(ColumnBlock block = new ColumnBlock();) {
                for (int i = 0; i < columnNames.length; i++) {
                    String columnName = columnNames[i];
                    String[] singleColumnData = columnData[i];
                    log.info("DingoColumnStorage in for loop, i: {}, singleColumnData count: {}", i, singleColumnData.length);
                    for (int j = 0; j < singleColumnData.length; j++) {
                        log.info("DingoColumnStorage in for loop, singleColumnData[{}]: {}", j, singleColumnData[j]);
                    }
                    int ret = block.appendData(database, table, columnName, singleColumnData);
                    if (ret < 0) {
                        log.error("block append data error while insert table , error code: " + ret);
                        return;
                    }
                }
                final String sessionID = UUID.randomUUID().toString();
                int ret = block.blockInsertTable(sessionID, database, table);
                if (ret < 0) {
                    log.error("block append data error while insert table , error code: " + ret);
                    return;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                storageWriter.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public int dropTable() {
        final String sessionID = UUID.randomUUID().toString();
        log.info("DingoColumnStorage dropTable, sessionID: {}, table: {}.", sessionID, this.table);
        columnStorage.dropTable(sessionID, this.database, this.table);
        return 0;
    }

    public int merge() {
        final String sessionID = UUID.randomUUID().toString();
        log.info("DingoColumnStorage merge,sessionID: {}, table: {}", sessionID, this.table);
        int ret = columnStorage.mergeTable(sessionID, this.database, this.table, "*");
        if (ret < 0) {
            throw new RuntimeException("merge table error, ret: " + ret);
        }
        return 0;
    }

    private static ColumnFamilyDescriptor icfDesc(ColumnFamilyOptions cfOptions) {
        return new ColumnFamilyDescriptor(CF_DEFAULT, cfOptions);
    }

    private static ColumnFamilyDescriptor mcfDesc() {
        return new ColumnFamilyDescriptor(CF_DEFAULT, new ColumnFamilyOptions());
    }
}
