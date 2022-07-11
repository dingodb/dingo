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
import io.dingodb.raft.kv.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.zip.Checksum;

@Slf4j
public class MemoryRawKVStore implements RawKVStore {

    private static final Comparator<byte[]> COMPARATOR = ByteArrayUtils::compare;

    private final ConcurrentNavigableMap<byte[], ByteArrayEntry> defaultDB = new ConcurrentSkipListMap<>(COMPARATOR);
    private final AtomicBoolean snapshotDoing = new AtomicBoolean(false);

    protected ConcurrentNavigableMap<byte[], ByteArrayEntry> defaultDB() {
        return defaultDB;
    }

    @Override
    public void close() {
        RawKVStore.super.close();
        defaultDB.clear();
    }

    @Override
    public SeekableIterator<byte[], ByteArrayEntry> iterator() {
        if (defaultDB.isEmpty()) {
            return ByteArrayEntryIterator.EMPTY;
        }
        return new ByteArrayEntryIterator(new TreeMap<>(defaultDB));
    }

    @Override
    public byte[] get(byte[] key) {
        return defaultDB.get(key).getValue();
    }

    @Override
    public List<ByteArrayEntry> get(List<byte[]> keys) {
        return keys.stream().filter(Objects::nonNull).map(defaultDB::get).collect(Collectors.toList());
    }

    @Override
    public boolean containsKey(byte[] key) {
        return defaultDB.containsKey(key);
    }

    @Override
    public SeekableIterator<byte[], ByteArrayEntry> scan(byte[] startKey, byte[] endKey) {
        if (defaultDB.isEmpty()) {
            return ByteArrayEntryIterator.EMPTY;
        }
        if (startKey == null && endKey == null) {
            return iterator();
        }
        if (startKey == null) {
            return new ByteArrayEntryIterator(defaultDB.headMap(endKey, false));
        }
        if (endKey == null) {
            return new ByteArrayEntryIterator(defaultDB.tailMap(startKey, true));
        }
        return new ByteArrayEntryIterator(defaultDB.subMap(startKey, true, endKey, false));
    }

    @Override
    public void put(byte[] key, byte[] value) {
        defaultDB.put(key, new ByteArrayEntry(key, value));
    }

    @Override
    public void put(List<ByteArrayEntry> entries) {
        entries.forEach(e -> defaultDB.put(e.getKey(), e));
    }

    @Override
    public boolean delete(byte[] key) {
        defaultDB.remove(key);
        return true;
    }

    @Override
    public boolean delete(List<byte[]> keys) {
        keys.forEach(defaultDB::remove);
        return true;
    }

    @Override
    public boolean delete(byte[] startKey, byte[] endKey) {
        defaultDB.subMap(startKey, true, endKey, false).keySet().forEach(defaultDB::remove);
        return true;
    }

    @Override
    public long count(byte[] startKey, byte[] endKey) {
        return defaultDB.subMap(startKey, true, endKey, false).size();
    }

    @Override
    public byte[] compute(byte[] key, byte[] computes) {
        return null;
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

    public Checksum snapshotSaveSync(String path, byte[] startKey, byte[] endKey) throws Exception {
        try {
            if (!snapshotDoing.compareAndSet(false, true)) {
                return null;
            }
            Files.createDirectories(Paths.get(path));
            Path snapshotTmpPath = Paths.get(path, Constants.SNAPSHOT);
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 4);

            FileChannel channel = FileChannel.open(snapshotTmpPath, new StandardOpenOption[] {
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE
            });

            SeekableIterator<byte[], ByteArrayEntry> iterator = iterator();
            if (startKey != null) {
                iterator.seek(startKey);
            }
            while (iterator.hasNext()) {
                ByteArrayEntry entry = iterator.next();
                if (endKey != null && ByteArrayUtils.compare(entry.getKey(), endKey) >= 0) {
                    break;
                }
                write(entry.getKey(), buffer, channel);
                write(entry.getValue(), buffer, channel);
            }
            channel.write((ByteBuffer) buffer.flip());
            channel.close();

            Checksum checksum = ParallelZipCompressor.compress(
                new File(snapshotTmpPath.toString()),
                new File(Paths.get(path, Constants.SNAPSHOT_ZIP).toString())
            );
            Files.delete(snapshotTmpPath);
            return checksum;
        }  finally {
            snapshotDoing.set(false);
        }
    }

    @Override
    public CompletableFuture<Boolean> snapshotLoad(String path, String checksum) {
        return snapshotLoad(path, checksum, null, null);
    }

    @Override
    public CompletableFuture<Boolean> snapshotLoad(String path, String checksum, byte[] startKey, byte[] endKey) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        new Thread(() -> {
            future.complete(snapshotLoadSync(path, checksum, null, null));
        }).start();
        return future;
    }

    public boolean snapshotLoadSync(String path, String checksum, byte[] startKey, byte[] endKey) {
        try {
            if (!snapshotDoing.compareAndSet(false, true)) {
                return false;
            }
            Path snapshotPath = Paths.get(path, Constants.SNAPSHOT);
            ParallelZipCompressor.deCompress(Paths.get(path, Constants.SNAPSHOT_ZIP).toString(), path ,checksum);

            ByteBuffer buffer = ByteBuffer.allocate(1024 * 4);
            buffer.limit(0);
            FileChannel channel = FileChannel.open(snapshotPath, StandardOpenOption.READ);

            byte[] key;
            byte[] value;
            while (true) {
                key = read(buffer, channel);
                if (key == null) {
                    break;
                }
                value = read(buffer, channel);
                if (value == null) {
                    break;
                }
                if (startKey != null && ByteArrayUtils.compare(key, startKey) < 0) {
                    continue;
                }
                if (endKey != null && ByteArrayUtils.compare(key, endKey) >= 0) {
                    break;
                }
                defaultDB.put(key, new ByteArrayEntry(key, value));
            }
            channel.close();
            FileUtils.forceDeleteOnExit(new File(snapshotPath.toString()));
            return true;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            snapshotDoing.set(false);
        }
    }

    private Integer readInt(ByteBuffer buffer, ReadableByteChannel channel) throws IOException {
        Integer value = PrimitiveCodec.readVarInt(buffer);
        if (value == null) {
            channel.read((ByteBuffer)buffer.clear());
            buffer.flip();
            value = PrimitiveCodec.readVarInt(buffer);
        }
        return value;
    }

    private byte[] read(ByteBuffer buffer, ReadableByteChannel channel) throws IOException {
        Integer len = readInt(buffer, channel);
        if (len == null) {
            return null;
        }
        if (buffer.remaining() < len) {
            channel.read((ByteBuffer)buffer.clear());
            if (buffer.flip().limit() < 1) {
                return null;
            }
        }
        if (len == 0) {
            return ByteArrayUtils.EMPTY_BYTES;
        }
        byte[] content = new byte[len];
        if (buffer.remaining() < len) {
            int index = 0;
            int remaining;
            int size;
            do {
                remaining = buffer.remaining();
                buffer.get(content, index, size = Math.min(remaining, len - index));
                index += size;
                channel.read((ByteBuffer)buffer.clear());
                buffer.flip();
            }
            while (len > index);
            return content;
        }
        buffer.get(content);
        return content;
    }

    private void writeInt(int value, ByteBuffer buffer, WritableByteChannel channel) throws IOException {
        byte[] len = PrimitiveCodec.encodeVarInt(value);
        if (buffer.remaining() < len.length) {
            channel.write((ByteBuffer) buffer.flip());
            buffer.clear();
        }
        buffer.put(len);
    }

    private void write(byte[] content, ByteBuffer buffer, WritableByteChannel channel) throws IOException {
        if (content == null) {
            writeInt(0, buffer, channel);
            return;
        }
        int len = content.length;
        writeInt(len, buffer, channel);
        if (buffer.remaining() < len) {
            channel.write((ByteBuffer) buffer.flip());
            buffer.clear();
        }
        if (buffer.remaining() < len) {
            int remaining;
            int index = 0;
            byte[] segment;
            int size;
            do {
                remaining = buffer.remaining();
                segment = new byte[remaining];
                System.arraycopy(content, index, segment, 0, size = Math.min(remaining, len - index));
                buffer.put(segment);
                index += size;
            }
            while (len > index && channel.write((ByteBuffer) buffer.flip()) > 0 && buffer.clear() != null);
            return;
        }
        buffer.put(content);
    }

    static class ByteArrayEntryIterator implements SeekableIterator<byte[], ByteArrayEntry> {
        public static final ByteArrayEntryIterator EMPTY = new ByteArrayEntryIterator(null);

        private final NavigableMap<byte[], ByteArrayEntry> db;

        private Iterator<ByteArrayEntry> currentIterator;
        private ByteArrayEntry currentEntry;

        public ByteArrayEntryIterator(NavigableMap<byte[], ByteArrayEntry> db) {
            this.db = db;
            seekToFirst();
        }

        @Override
        public ByteArrayEntry current() {
            return currentEntry;
        }

        @Override
        public void seek(byte[] position) {
            if (db == null) {
                return;
            }
            currentIterator = db.tailMap(position, true).values().iterator();
        }

        @Override
        public void seekToFirst() {
            if (db == null) {
                return;
            }
            currentIterator = this.db.values().iterator();
        }

        @Override
        public void seekToLast() {
            if (db == null) {
                return;
            }
            currentIterator = Collections.singleton(db.lastEntry().getValue()).iterator();
        }

        @Override
        public boolean hasNext() {
            if (db == null) {
                return false;
            }
            return currentIterator.hasNext();
        }

        @Override
        public ByteArrayEntry next() {
            if (db == null) {
                return null;
            }
            return currentEntry = currentIterator.next();
        }

    }

}
