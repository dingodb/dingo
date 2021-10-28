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

package io.dingodb.kvstore;

import org.rocksdb.RocksDB;

import java.util.Iterator;
import javax.annotation.Nonnull;

public class RocksBlockIterator implements Iterator<KeyValue> {
    private final org.rocksdb.RocksIterator iterator;

    public RocksBlockIterator(@Nonnull RocksDB db) {
        iterator = db.newIterator();
        iterator.seekToFirst();
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
        return iterator.isValid();
    }
}
