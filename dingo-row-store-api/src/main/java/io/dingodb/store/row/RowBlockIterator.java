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

package io.dingodb.store.row;

import io.dingodb.store.api.KeyValue;
import io.dingodb.store.row.client.DefaultDingoRowStore;
import io.dingodb.store.row.client.DefaultDingoRowStoreIterator;
import io.dingodb.store.row.client.DingoRowStoreIterator;
import io.dingodb.store.row.storage.KVEntry;

import java.util.Arrays;
import java.util.Iterator;
import javax.annotation.Nonnull;

public class RowBlockIterator implements Iterator<KeyValue> {
    private final DingoRowStoreIterator<KVEntry> iterator;

    public RowBlockIterator(@Nonnull DefaultDingoRowStore kvStore, @Nonnull byte[] keyPrefix) {
        int length = keyPrefix.length;
        byte[] keyEnd = Arrays.copyOf(keyPrefix, length);
        ++keyEnd[length - 1];
        iterator = kvStore.iterator(keyPrefix, keyEnd, 1024);
    }

    @Override
    public KeyValue next() {
        KVEntry kvEntry = ((DefaultDingoRowStoreIterator) iterator).next();
        byte[] key = kvEntry.getKey();
        byte[] value = kvEntry.getValue();
        return new KeyValue(key, value);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }
}
