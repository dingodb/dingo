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

import io.dingodb.dingokv.client.DefaultDingoKVIterator;
import io.dingodb.dingokv.client.DefaultDingoKVStore;
import io.dingodb.dingokv.client.DingoKVIterator;
import io.dingodb.dingokv.storage.KVEntry;

import java.util.Iterator;
import javax.annotation.Nonnull;

public class DingoKvBlockIterator implements Iterator<KeyValue> {
    private final DingoKVIterator iterator;

    public DingoKvBlockIterator(@Nonnull DefaultDingoKVStore kvStore) {
        iterator = kvStore.iterator(new byte[]{0}, new byte[]{Byte.MAX_VALUE}, 1024);
    }

    @Override
    public KeyValue next() {
        KVEntry kvEntry = ((DefaultDingoKVIterator) iterator).next();
        byte[] key = kvEntry.getKey();
        byte[] value = kvEntry.getValue();
        return new KeyValue(key, value);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }
}
