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

package io.dingodb.store.memory;

import io.dingodb.common.store.KeyValue;

import java.util.Iterator;
import java.util.Map;

public class KeyValueIterator implements Iterator<KeyValue> {

    private final Iterator<Map.Entry<byte[], byte[]>> iterator;

    public KeyValueIterator(Iterator<Map.Entry<byte[], byte[]>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public KeyValue next() {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        return new KeyValue(entry.getKey(), entry.getValue());
    }
}
