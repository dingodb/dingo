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

package io.dingodb.raft.kv.storage;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.StackTraces;
import io.dingodb.common.util.Utils;
import io.dingodb.raft.kv.Constants;
import io.dingodb.raft.util.BytesUtil;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ConfigOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.OptionsUtil;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
import org.rocksdb.SstFileWriter;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.zip.Checksum;
import javax.annotation.Nonnull;

import static io.dingodb.common.codec.PrimitiveCodec.readZigZagInt;
import static io.dingodb.common.util.ByteArrayUtils.greatThanOrEqual;
import static io.dingodb.raft.storage.impl.RocksDBLogStore.createColumnFamilyOptions;

@Slf4j
public class RocksRawKVStore implements RawKVStore {

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB db;
    private WriteOptions writeOptions;

    public RocksRawKVStore(final String dataPath, final String optionsFile) throws RocksDBException {
        this.writeOptions = new WriteOptions();
        DBOptions options = new DBOptions();
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

        boolean useDefaultOptions = true;
        try {
            if (optionsFile != null && (new File(optionsFile)).exists()) {
                log.info("RocksRawKVStore rocksdb config file found: {}.", optionsFile);
                ConfigOptions configOptions = new ConfigOptions();
                OptionsUtil.loadOptionsFromFile(configOptions, optionsFile, options, columnFamilyDescriptors);
                useDefaultOptions = false;
            } else {
                log.info("RocksRawKVStore rocksdb options file not found: {}, use default options.", optionsFile);
            }
        } catch (RocksDBException re) {
            log.warn("RocksRawKVStore, load {} exception, use default options.", optionsFile, re);
        }

        options.setCreateIfMissing(true);
        if (useDefaultOptions) {
            final ColumnFamilyOptions cfOption = createColumnFamilyOptions();
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOption));
        }

        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        this.db = RocksDB.open(options, dataPath, columnFamilyDescriptors, columnFamilyHandles);
        log.info("RocksRawKVStore RocksDB open, path: {}, options file: {}, columnFamilyHandles size: {}.",
            dataPath, optionsFile, columnFamilyHandles.size());
    }

    @Override
    public void close() {
        RawKVStore.super.close();
        this.db.close();
    }

    @Override
    public SeekableIterator<byte[], ByteArrayEntry> iterator() {
        try (ReadOptions readOptions = new ReadOptions()) {
            try (Snapshot snapshot = this.db.getSnapshot()) {
                readOptions.setSnapshot(snapshot);
                return new ByteArrayEntryIterator(
                    db.newIterator(readOptions), null, null, true, true);
            }
        }
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ByteArrayEntry> get(List<byte[]> keys) {
        try {
            List<byte[]> values = db.multiGetAsList(keys);
            List<ByteArrayEntry> entries = new ArrayList<>(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                entries.add(new ByteArrayEntry(keys.get(i), values.get(i)));
            }
            return entries;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean containsKey(byte[] key) {
        try {
            return db.get(key) != null;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SeekableIterator<byte[], ByteArrayEntry> scan(byte[] startKey, byte[] endKey) {
        return scan(startKey, endKey, true, false);
    }

    public SeekableIterator<byte[], ByteArrayEntry> scan(
        byte[] startKey, byte[] endKey, boolean includeStart, boolean includeEnd
    ) {
        try (ReadOptions readOptions = new ReadOptions()) {
            try (Snapshot snapshot = this.db.getSnapshot()) {
                readOptions.setSnapshot(snapshot);
                return new ByteArrayEntryIterator(
                    db.newIterator(readOptions), startKey, endKey, includeStart, includeEnd);
            }
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(List<ByteArrayEntry> entries) {
        try (final WriteBatch batch = new WriteBatch()) {
            for (final ByteArrayEntry entry : entries) {
                batch.put(entry.getKey(), entry.getValue());
            }
            this.db.write(this.writeOptions, batch);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean delete(byte[] key) {
        try {
            db.delete(key);
            return true;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean delete(List<byte[]> keys) {
        try (final WriteBatch batch = new WriteBatch()) {
            for (final byte[] key : keys) {
                batch.delete(key);
            }
            this.db.write(this.writeOptions, batch);
            return true;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean delete(byte[] startKey, byte[] endKey) {
        try {
            if (endKey == null) {
                try (RocksIterator iterator = this.db.newIterator();) {
                    iterator.seekToLast();
                    if (iterator.isValid()) {
                        try (final WriteBatch batch = new WriteBatch()) {
                            endKey = iterator.key();
                            batch.delete(endKey);
                            batch.deleteRange(startKey, endKey);
                            this.db.write(this.writeOptions, batch);
                            this.db.deleteFilesInRanges(this.db.getDefaultColumnFamily(), Arrays.asList(startKey, endKey), true);
                            this.db.compactRange(startKey, endKey);
                            return true;
                        }
                    } else {
                        log.warn("DB not have last key, may be db is empty.");
                        return true;
                    }
                }
            } else {
                this.db.deleteFilesInRanges(this.db.getDefaultColumnFamily(), Arrays.asList(startKey, endKey), true);
                this.db.deleteRange(this.writeOptions, startKey, endKey);
                this.db.compactRange(startKey, endKey);
                return true;
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long count(byte[] startKey, byte[] endKey) {
        long count = 0;
        try (
            ReadOptions readOptions = new ReadOptions();
            Snapshot snapshot = this.db.getSnapshot();
            RocksIterator iterator = db.newIterator(readOptions.setSnapshot(snapshot))
        ) {
            if (startKey == null) {
                iterator.seekToFirst();
            } else {
                iterator.seek(startKey);
            }
            while (iterator.isValid()) {
                if (endKey != null && greatThanOrEqual(iterator.key(), endKey)) {
                    break;
                }
                count++;
                iterator.next();
            }
        }
        return count;
    }

    @Override
    public CompletableFuture<Checksum> snapshotSave(String path) {
        return snapshotSave(path, null, null);
    }

    @Override
    public CompletableFuture<Checksum> snapshotSave(String path, byte[] startKey, byte[] endKey) {
        CompletableFuture<Checksum> future = new CompletableFuture<>();
        new Thread(() -> {
            try {
                future.complete(snapshotSaveSync(path, startKey, endKey));
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }).start();
        return future;
    }

    @Nonnull
    private Checksum snapshotSaveSync(String path, byte[] startKey, byte[] endKey) throws Exception {
        log.info("RocksRawKVStore snapshotSaveSync, path: {}, startKey: {}, endKey: {}.",
            path, BytesUtil.toHex(startKey), BytesUtil.toHex(endKey));
        Path sstPath = Paths.get(path, Constants.SNAPSHOT_SST);
        Files.createDirectories(Paths.get(path));
        try (
            Snapshot snapshot = this.db.getSnapshot();
            ReadOptions readOptions = new ReadOptions();
            EnvOptions envOptions = new EnvOptions();
            Options options = new Options();
        ) {
            readOptions.setSnapshot(snapshot);
            try (
                RocksIterator iterator = this.db.newIterator(readOptions);
                SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options)
            ) {
                sstFileWriter.open(sstPath.toAbsolutePath().toString());
                if (startKey == null) {
                    iterator.seekToFirst();
                } else {
                    iterator.seek(startKey);
                }
                int count = 0;
                while (true) {
                    if (!iterator.isValid()) {
                        break;
                    }
                    byte[] key = iterator.key();
                    if (endKey != null && ByteArrayUtils.compare(iterator.key(), endKey) >= 0) {
                        break;
                    }
                    byte[] value = iterator.value();
                    sstFileWriter.put(key, value);
                    iterator.next();
                    count++;
                }
                if (count == 0) {
                    sstFileWriter.close();
                } else {
                    sstFileWriter.finish();
                }
            }
        }
        Checksum checksum = ParallelZipCompressor.compress(
            new File(sstPath.toString()),
            new File(Paths.get(path, Constants.SNAPSHOT_ZIP).toString())
        );
        Files.delete(sstPath);
        return checksum;
    }

    @Override
    public CompletableFuture<Boolean> snapshotLoad(String path, String checksum) {
        return snapshotLoad(path, checksum, null, null);
    }

    @Override
    public CompletableFuture<Boolean> snapshotLoad(String path, String checksum, byte[] startKey, byte[] endKey) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        new Thread(() -> {
            try {
                future.complete(snapshotLoadSync(path, checksum, startKey, endKey));
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }).start();
        return future;
    }

    public Boolean snapshotLoadSync(String path, String checksum, byte[] startKey, byte[] endKey) throws Exception {
        Path sstPath = Paths.get(path, Constants.SNAPSHOT_SST);
        ParallelZipCompressor.deCompress(Paths.get(path, Constants.SNAPSHOT_ZIP).toString(), path ,checksum);
        if (Files.size(sstPath) == 0) {
            if (log.isDebugEnabled()) {
                log.warn("Load snapshot file {}, but sst file size is 0, skip load and return true.", sstPath);
            }
            return true;
        }
        try (
            ReadOptions readOptions = new ReadOptions();
            Options options = new Options();
            SstFileReader reader = new SstFileReader(options)
        ) {
            reader.open(sstPath.toAbsolutePath().toString());
            try (SstFileReaderIterator iterator = reader.newIterator(readOptions)) {
                if (startKey == null) {
                    iterator.seekToFirst();
                } else {
                    iterator.seek(startKey);
                }
                while (true) {
                    byte[] key = iterator.key();
                    if (endKey != null && ByteArrayUtils.compare(iterator.key(), endKey) >= 0) {
                        break;
                    }
                    byte[] value = iterator.value();
                    this.db.put(key, value);
                    iterator.next();
                    if (!iterator.isValid()) {
                        break;
                    }
                }
            }
        } finally {
            Files.delete(sstPath);
        }
        return true;
    }

    @SuppressWarnings("checkstyle:NoFinalizer")
    static class ByteArrayEntryIterator implements SeekableIterator<byte[], ByteArrayEntry> {

        private final byte[] startKey;
        private final byte[] endKey;
        private final Predicate<byte[]> compareWithStart;
        private final Predicate<byte[]> compareWithEnd;
        private final RocksIterator iterator;
        private final int buffSize;
        private Iterator<ByteArrayEntry> buffer;

        private boolean hasMore = true;
        private ByteArrayEntry currentEntry;

        public ByteArrayEntryIterator(
            RocksIterator iterator, byte[] startKey, byte[] endKey, boolean includeStart, boolean includeEnd
        ) {
            this.iterator = iterator;
            this.startKey = startKey;
            this.endKey = endKey;
            this.buffSize = 100;

            if (includeStart) {
                compareWithStart = start -> startKey == null || ByteArrayUtils.greatThanOrEqual(start, startKey);
            } else {
                compareWithStart = start -> startKey == null || ByteArrayUtils.greatThan(start, startKey);
            }

            if (includeEnd) {
                compareWithEnd = end -> endKey == null || ByteArrayUtils.lessThanOrEqual(end, endKey);
            } else {
                compareWithEnd = end -> endKey == null || ByteArrayUtils.lessThan(end, endKey);
            }
            seekToFirst();
        }

        private synchronized void load() {
            if (!hasMore) {
                return;
            }
            ArrayList<ByteArrayEntry> list = new ArrayList<>();
            byte[] key = ByteArrayUtils.EMPTY_BYTES;
            for (int i = 0; i < buffSize; i++, iterator.next()) {
                if (!iterator.isValid() || !compareWithEnd.test(key = iterator.key())) {
                    hasMore = false;
                    break;
                }
                list.add(new ByteArrayEntry(iterator.key(), iterator.value()));
            }
            buffer = list.iterator();
        }

        @Override
        public ByteArrayEntry current() {
            return currentEntry;
        }

        @Override
        public void seek(byte[] position) {
            if ((startKey == null || ByteArrayUtils.greatThanOrEqual(position, startKey))
                && (endKey == null || ByteArrayUtils.lessThanOrEqual(position, endKey))) {
                iterator.seek(position);
                if (iterator.isValid() && !compareWithStart.test(position)) {
                    iterator.next();
                }
                if (iterator.isValid() && !compareWithEnd.test(position)) {
                    iterator.prev();
                }
                load();
            } else {
                throw new IllegalArgumentException("Position out of range.");
            }
        }

        @Override
        public void seekToFirst() {
            if (iterator == null) {
                throw new RuntimeException("Iterator is null.");
            }
            if (startKey == null) {
                iterator.seekToFirst();
                load();
            } else {
                seek(startKey);
            }
        }

        @Override
        public void seekToLast() {
            if (iterator == null) {
                throw new RuntimeException("Iterator is null.");
            }
            if (endKey == null) {
                if (hasMore) {
                    iterator.seekToLast();
                } else {
                    while (hasNext()) {
                        next();
                    }
                }
            } else {
                if (hasMore) {
                    iterator.seek(endKey);
                } else {
                    Utils.emptyWhile(() -> hasNext() && ByteArrayUtils.compare(next().getKey(), endKey) < 0);
                }
            }
            load();
        }

        @Override
        public boolean hasNext() {
            if (buffer.hasNext()) {
                return true;
            }
            if (hasMore) {
                load();
            }
            return buffer.hasNext();
        }

        @Override
        public ByteArrayEntry next() {
            return currentEntry = buffer.next();
        }

        // close rocksdb iterator
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            try {
                iterator.close();
            } catch (Exception e) {
                log.error("Close iterator on finalize error.", e);
            }
        }
    }

}
