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

package io.dingodb.store.local;

import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.dingodb.common.util.ByteArrayUtils.compareWithoutLen;
import static io.dingodb.common.util.Parameters.cleanNull;
import static io.dingodb.common.util.Parameters.nonNull;

@AllArgsConstructor
public class StoreInstance implements io.dingodb.store.api.StoreInstance {

    public final CommonId regionId;
    private static final WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);

    @Override
    public CommonId id() {
        return regionId;
    }

    @Override
    @SneakyThrows
    public boolean put(KeyValue row) {
        nonNull(row, "row");
        if (StoreService.db.get(row.getKey()) != null) {
            return false;
        }
        StoreService.db.put(writeOptions, nonNull(row.getKey(), "key"), cleanNull(row.getValue(), ByteArrayUtils.EMPTY_BYTES));
        return true;
    }

    @Override
    @SneakyThrows
    public boolean delete(byte[] key) {
        StoreService.db.delete(writeOptions, key);
        return true;
    }

    @Override
    @SneakyThrows
    public void deletePrefix(byte[] prefix) {
        StoreService.db.deleteRange(writeOptions, prefix, nextKey(prefix));
    }

    @Override
    @SneakyThrows
    public KeyValue get(byte[] key) {
        byte[] valueBytes = StoreService.db.get(key);
        if (valueBytes == null) {
            return null;
        }
        return new KeyValue(key, valueBytes);
    }

    @Override
    @SneakyThrows
    public List<KeyValue> get(List<byte[]> keys) {
        List<byte[]> values = StoreService.db.multiGetAsList(keys);
        return IntStream.range(0, keys.size())
            .mapToObj(i -> new KeyValue(keys.get(i), values.get(i)))
            .filter(kv -> kv.getValue() != null)
            .collect(Collectors.toList());
    }

    @Override
    public Iterator<KeyValue> scan(Range range) {
        return new KeyValueIterator(StoreService.db.newIterator(), range);
    }

    @Override
    public Iterator<KeyValue> scan(long requestTs, Range range) {
        return new KeyValueIterator(StoreService.db.newIterator(), range);
    }

    private byte[] nextKey(byte[] key) {
        byte[] next = new byte[key.length];
        int n = key.length;
        while (--n >= 0) {
            if (key[n] != (byte) 0xFF) {
                next[n] = (byte) (key[n] + 1);
                break;
            }
        }
        System.arraycopy(key, 0, next, 0, n);
        return next;
    }

    public class KeyValueIterator implements Iterator<KeyValue> {

        private final RocksIterator iterator;
        private final Range range;

        private final byte[] end;
        private boolean hasNext;

        public KeyValueIterator(RocksIterator iterator, Range range) {
            this.iterator = iterator;
            this.range = range;
            if (range.start == null) {
                iterator.seekToFirst();
            } else {
                if (range.withStart) {
                    iterator.seek(range.start);
                } else {
                    iterator.seek(nextKey(range.start));
                }
            }
            if (range.end == null) {
                end = ByteArrayUtils.MAX;
            } else {
                end = range.withEnd ? nextKey(range.end) : range.end;
            }
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            iterator.close();
        }

        @Override
        public boolean hasNext() {
            return hasNext = iterator.isValid() && compareWithoutLen(iterator.key(), end) < 0;
        }

        @Override
        public KeyValue next() {
            if (!hasNext) {
                throw new NoSuchElementException();
            }
            KeyValue keyValue = new KeyValue(iterator.key(), iterator.value());
            iterator.next();
            return keyValue;
        }
    }
}
