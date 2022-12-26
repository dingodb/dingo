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

package io.dingodb.mpu.storage.mem;

import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.instruction.Instruction;
import io.dingodb.mpu.storage.Storage;
import org.rocksdb.RocksIterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.common.util.ByteArrayUtils.lessThan;
import static io.dingodb.common.util.ByteArrayUtils.lessThanOrEqual;

public class MemStorage implements Storage {

    protected final NavigableMap<byte[], byte[]> db = new TreeMap<>(ByteArrayUtils::compare);
    private final NavigableMap<Long, byte[]> instructions = new TreeMap<>();
    private long clocked;

    @Override
    public long clocked() {
        return clocked;
    }

    @Override
    public long clock() {
        return clocked;
    }

    @Override
    public void tick(long clock) {

    }

    @Override
    public void saveInstruction(long clock, byte[] instruction) {
        instructions.put(clock, instruction);
    }

    @Override
    public byte[] reappearInstruction(long clock) {
        return instructions.get(clock);
    }

    @Override
    public void destroy() {

    }

    @Override
    public CompletableFuture<Void> transferTo(CoreMeta meta) {
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

    }

    @Override
    public void clearClock(long clock) {

    }

    @Override
    public long approximateCount() {
        return db.size();
    }

    @Override
    public long approximateSize() {
        return db.entrySet().stream()
            .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
            .map(__ -> __.length)
            .reduce(Integer::sum)
            .orElse(0);
    }

    @Override
    public Reader reader() {
        return new io.dingodb.mpu.storage.mem.Reader(this);
    }

    @Override
    public Writer writer(Instruction instruction) {
        return new Writer(instruction);
    }

    @Override
    public void flush(io.dingodb.mpu.storage.Writer writerT) {
        Writer writer = (Writer) writerT;
        db.putAll(writer.cache);
        if (writer.eraseRange) {
            TreeMap<byte[], byte[]> treeMap = getTreeMapByRange(
                db.entrySet().iterator(), writer.eraseStart, writer.eraseEnd, true, false);
            treeMap.keySet().forEach(db::remove);
        }
        clocked = writer.instruction().clock;
    }

    protected TreeMap<byte[], byte[]> getTreeMapByRange(
        Iterator<Map.Entry<byte[], byte[]>> iterator,
        byte[] startPrimaryKey,
        byte[] endPrimaryKey,
        boolean includeStart,
        boolean includeEnd) {
        TreeMap<byte[], byte[]> treeMap = new TreeMap<>(ByteArrayUtils::compareWithoutLen);

        while (iterator.hasNext()) {
            Map.Entry<byte[], byte[]> next = iterator.next();
            boolean start = true;
            boolean end = false;
            if (includeStart) {
                start = ByteArrayUtils.greatThanOrEqual(next.getKey(), startPrimaryKey);
            } else {
                start = ByteArrayUtils.greatThan(next.getKey(), startPrimaryKey);
            }

            if (includeEnd) {
                end = ByteArrayUtils.lessThanOrEqual(next.getKey(), endPrimaryKey);
            } else {
                end = ByteArrayUtils.lessThan(next.getKey(), endPrimaryKey);
            }

            if (start && end) {
                treeMap.put(next.getKey(), next.getValue());
            }
        }
        return treeMap;
    }

    public Iterator<KeyValue> scan(byte[] startKey, byte[] endKey, boolean withStart, boolean withEnd) {
        if (endKey == null) {
            endKey = db.lastKey();
        }

        return new KeyValueIterator(startKey, endKey, withStart, withEnd);
    }

    public long count(byte[] startKey, byte[] endKey) {
        return db.size();
    }

    public byte[] get(byte[] key) {
        return db.get(key);
    }

    public List<KeyValue> get(List<byte[]> keys) {
        return keys.stream().map(k -> new KeyValue(k, db.get(k))).collect(Collectors.toList());
    }

    class KeyValueIterator implements Iterator<KeyValue> {

        private Iterator<Map.Entry<byte[], byte[]>> iterator;
        private final Predicate<byte[]> _end;
        private Map.Entry<byte[], byte[]> current;

        KeyValueIterator(byte[] start, byte[] end, boolean withStart, boolean withEnd) {
            this._end = end == null ? __ -> true : withEnd ? __ -> lessThanOrEqual(__, end) : __ -> lessThan(__, end);
            if (start == null) {
                this.iterator = db.tailMap(db.firstKey(), true).entrySet().iterator();
            } else {
                this.iterator = db.tailMap(start, withStart).entrySet().iterator();
            }
            if (iterator.hasNext()) {
                current = iterator.next();
            }
        }

        @Override
        public boolean hasNext() {
            return current != null && _end.test(current.getKey());
        }

        @Override
        public KeyValue next() {
            if (current == null) {
                throw new RuntimeException();
            }
            KeyValue result = new KeyValue(current.getKey(), current.getValue());
            if (iterator.hasNext()) {
                current = iterator.next();
            } else {
                current = null;
            }
            return result;
        }

    }

}
