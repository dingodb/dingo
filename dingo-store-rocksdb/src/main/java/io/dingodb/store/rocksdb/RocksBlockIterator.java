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

package io.dingodb.store.rocksdb;

import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.store.api.KeyValue;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDB;

import java.util.Iterator;
import javax.annotation.Nonnull;

@Slf4j
public class RocksBlockIterator implements Iterator<KeyValue> {
    private final org.rocksdb.RocksIterator iterator;

    private final byte[] start;
    private final byte[] end;

    public RocksBlockIterator(@Nonnull RocksDB db, byte[] start, byte[] end) {
        iterator = db.newIterator();
        this.start = start;
        this.end = end;
        if (start == null) {
            iterator.seekToFirst();
        } else {
            iterator.seek(start);
        }
    }

    @Override
    public KeyValue next() {
        byte[] key = iterator.key();
        byte[] value = iterator.value();
        iterator.next();
        return new KeyValue(key, value);
    }

    @Override
    public boolean hasNext() {
        return iterator.isValid() && (end == null || ByteArrayUtils.compare(iterator.key(), end) < 0);
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
