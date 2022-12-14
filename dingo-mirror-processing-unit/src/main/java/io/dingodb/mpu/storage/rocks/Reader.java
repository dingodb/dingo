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

import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.mpu.Constant;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.common.util.ByteArrayUtils.EMPTY_BYTES;
import static io.dingodb.common.util.ByteArrayUtils.greatThan;
import static io.dingodb.common.util.ByteArrayUtils.greatThanOrEqual;
import static io.dingodb.common.util.ByteArrayUtils.lessThan;
import static io.dingodb.common.util.ByteArrayUtils.lessThanOrEqual;

@Slf4j
@SuppressWarnings("checkstyle:NoFinalizer")
public class Reader implements io.dingodb.mpu.storage.Reader {
    private final RocksDB db;
    private final ColumnFamilyHandle handle;
    private final Snapshot snapshot;
    private final ReadOptions readOptions;

    public Reader(RocksDB db, ColumnFamilyHandle handle) {
        this.db = db;
        this.snapshot = db.getSnapshot();
        this.handle = handle;
        this.readOptions = new ReadOptions().setSnapshot(snapshot);
    }

    public Iterator iterator() {
        return new Iterator(db.newIterator(handle, readOptions), null, null, true, true);
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return db.get(handle, readOptions, key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<KeyValue> get(List<byte[]> keys) {
        try {
            List<byte[]> values = db.multiGetAsList(readOptions, keys);
            List<KeyValue> entries = new ArrayList<>(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                entries.add(new KeyValue(keys.get(i), values.get(i)));
            }
            return entries;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean containsKey(byte[] key) {
        try {
            return db.get(handle, readOptions, key) != null;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean maybePrefixScan(byte[] startKey, byte[] endKey) {
        if (startKey == null || endKey == null) {
            return false;
        }

        return Arrays.equals(ByteArrayUtils.increment(startKey), endKey);
    }

    @Override
    public Iterator scan(byte[] startKey, byte[] endKey, boolean withStart, boolean withEnd) {
        if (log.isDebugEnabled()) {
            log.debug("rocksdb reader scan: {} {} {} {}",
                startKey != null ? Arrays.toString(startKey) : "null",
                endKey != null ? Arrays.toString(endKey) : "null", withStart, withEnd);
        }

        if (maybePrefixScan(startKey, endKey)) {
            readOptions.setAutoPrefixMode(true);
            readOptions.setIterateUpperBound(new Slice(withEnd ? ByteArrayUtils.increment(endKey) : endKey));
        }

        return new Iterator(db.newIterator(handle, readOptions), startKey, endKey, withStart, withEnd);
    }

    // [start, end)
    private Map<byte[], byte[]> getRangeForCount(byte[] start, byte[] end) {
        Map<byte[], byte[]> result = new TreeMap<>(ByteArrayUtils::compare);
        java.util.Iterator<byte[]> keyIterator = db.getLiveFilesMetaData().stream()
            .filter(meta -> Arrays.equals(meta.columnFamilyName(), Constant.CF_DEFAULT))
            .flatMap(meta -> Stream.of(meta.smallestKey(), meta.largestKey()))
            .filter(k -> (end == null || lessThan(k, end)) && (start == null || greatThanOrEqual(k, start)))
            .collect(Collectors.toCollection(() -> new TreeSet<>(ByteArrayUtils::compare)))
            .iterator();
        if (!keyIterator.hasNext()) {
            return result;
        }

        byte[] key = keyIterator.next();
        if (start == null) {
            result.put(EMPTY_BYTES, key);
        } else if (lessThan(start, key)) {
            result.put(start, key);
        }
        while (keyIterator.hasNext()) {
            result.put(key, key = keyIterator.next());
        }
        if (end == null) {
            result.put(key, null);
        } else if (lessThan(key, end)) {
            result.put(key, end);
        }

        return result;
    }

    private long parallelCount(Map<byte[], byte[]> ranges) throws InterruptedException {
        AtomicLong count = new AtomicLong(0);
        CountDownLatch countDownLatch = new CountDownLatch(ranges.size());
        ranges.entrySet().forEach(entry -> {
            countAsync(entry.getKey(), entry.getValue())
                .whenComplete((r, e) -> {
                    try {
                        if (e == null) {
                            count.addAndGet(r);
                        } else {
                            log.error("Count {} sub {} to {} error.", db.getName(), entry.getKey(), entry.getValue());
                        }
                    } finally {
                        countDownLatch.countDown();
                    }
                });
        });

        countDownLatch.await();
        return count.get();
    }

    // [start, end)
    public long count(byte[] start, byte[] end) {
        if (log.isDebugEnabled()) {
            log.debug("rocksdb reader count: {} {}",
                start != null ? Arrays.toString(start) : "null",
                end != null ? Arrays.toString(end) : "null");
        }

        Map<byte[], byte[]> ranges = getRangeForCount(start, end);
        if (ranges.size() == 0) {
            return countAsync(start, end).join();
        }
        try {
            return parallelCount(ranges);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // [start, end)
    public CompletableFuture<Long> countAsync(byte[] start, byte[] end) {
        if (log.isDebugEnabled()) {
            log.debug("rocksdb reader countAsync: {} {}",
                start != null ? Arrays.toString(start) : "null",
                end != null ? Arrays.toString(end) : "null");
        }
        Predicate<byte[]> ep = end == null ? k -> true : k -> lessThan(k, end);
        return Executors.submit("calc-count", () -> {
            long count = 0;
            try (RocksIterator iterator = db.newIterator(handle, readOptions)) {
                if (start == null) {
                    iterator.seekToFirst();
                } else {
                    iterator.seek(start);
                }
                while (iterator.isValid() && ep.test(iterator.key())) {
                    count++;
                    iterator.next();
                }
            }
            return count;
        });
    }

    @Override
    public void close() {
        readOptions.setSnapshot(null);
        db.releaseSnapshot(snapshot);
        readOptions.close();
        snapshot.close();
    }

    static class Iterator implements java.util.Iterator<KeyValue> {
        private final RocksIterator iterator;
        private final Predicate<byte[]> _end;
        private final boolean isPrefixScan;

        Iterator(RocksIterator iterator, byte[] start, byte[] end, boolean withStart, boolean withEnd) {
            this.isPrefixScan = maybePrefixScan(start, end);
            this.iterator = iterator;
            this._end = end == null ? __ -> true : withEnd ? __ -> lessThanOrEqual(__, end) : __ -> lessThan(__, end);
            if (start == null) {
                this.iterator.seekToFirst();
            } else {
                this.iterator.seek(start);
            }
            if (this.iterator.isValid() && !withStart && Arrays.equals(this.iterator.key(), start)) {
                this.iterator.next();
            }
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid() && _end.test(iterator.key());
        }

        @Override
        public KeyValue next() {
            if (iterator.isValid() && _end.test(iterator.key())) {
                KeyValue kv = new KeyValue(iterator.key(), iterator.value());
                iterator.next();
                return kv;
            }

            return null;
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
