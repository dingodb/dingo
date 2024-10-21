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

package io.dingodb.exec.utils;

import com.google.common.base.Objects;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.store.api.transaction.data.Op;
import lombok.AllArgsConstructor;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class TxnMergedIterator implements Iterator<Object[]> {

    private final Iterator<KeyValue> localKVIterator;
    private final Iterator<KeyValue> kvKVIterator;
    private final KeyValueCodec decoder;
    private KeyValue nextLocal;
    private KeyValue nextKv;
    private KeyValue result;
    private Set<KeyBytes> deletedSet = new HashSet<>();

    public TxnMergedIterator(Iterator<KeyValue> localKVIterator, Iterator<KeyValue> kvKVIterator, KeyValueCodec decoder) {
        this.localKVIterator = localKVIterator;
        this.kvKVIterator = kvKVIterator;
        this.decoder = decoder;
        nextLocal = getNextValue(localKVIterator);
        nextKv = getNextValue(kvKVIterator);
    }

    @Override
    public boolean hasNext() {
        // Compute the next element after returning the current one
        computeNext();
        return result != null;
    }

    @Override
    public Object[] next() {
        // Decode the value using the provided codec
        Object[] decoded = decoder.decode(result);
        return decoded;
    }

    private void computeNext() {
        result = null;
        while (nextLocal != null && nextKv != null) {
            byte[] key1 = nextLocal.getKey();
            byte[] key2 = nextKv.getKey();
            int code = key1[key1.length - 2];
            if (code == Op.NONE.getCode()) {
                nextLocal = getNextValue(localKVIterator);
                continue;
            }
            if (ByteArrayUtils.lessThan(key1, key2, 9, key1.length - 2)) {
                if (
                    (code == Op.PUT.getCode() || code == Op.PUTIFABSENT.getCode())
                        && !deletedSet.contains(new KeyBytes(key2))
                ) {
                    result = nextLocal;
                    nextLocal = getNextValue(localKVIterator);
                    break;
                } else if (code == Op.DELETE.getCode()) {
                    deletedSet.add(new KeyBytes(key1));
                    nextLocal = getNextValue(localKVIterator);
                    continue;
                }
            }
            if (ByteArrayUtils.greatThan(key1, key2, 9, key1.length - 2)) {
                if (
                    (code == Op.PUT.getCode() || code == Op.PUTIFABSENT.getCode())
                        && !deletedSet.contains(new KeyBytes(key2))
                ) {
                    result = nextKv;
                    nextKv = getNextValue(kvKVIterator);
                    break;
                }
                if (code == Op.DELETE.getCode()) {
                    result = nextKv;
                    nextKv = getNextValue(kvKVIterator);
                    break;
                }
            }
            if (ByteArrayUtils.compare(key1, key2, 9, key1.length - 2) == 0) {
                if (code == Op.DELETE.getCode()) {
                    nextLocal = getNextValue(localKVIterator);
                    nextKv = getNextValue(kvKVIterator);
                    continue;
                }
                if ((code == Op.PUT.getCode() || code == Op.PUTIFABSENT.getCode())) {
                    result = nextLocal;
                    nextLocal = getNextValue(localKVIterator);
                    nextKv = getNextValue(kvKVIterator);
                    break;
                }
            }
        }
        if (result == null) {
            if (nextLocal == null && nextKv != null) {
                while (nextKv != null) {
                    if (nextKv.getKey()[nextKv.getKey().length - 2] != Op.DELETE.getCode()) {
                        result = nextKv;
                        nextKv = getNextValue(kvKVIterator);
                        break;
                    } else {
                        nextKv = getNextValue(kvKVIterator);
                    }
                }
            } else if (nextKv == null && nextLocal != null) {
                while (nextLocal != null) {
                    if (nextLocal.getKey()[nextLocal.getKey().length - 2] != Op.DELETE.getCode()) {
                        result = nextLocal;
                        nextLocal = getNextValue(localKVIterator);
                        break;
                    } else {
                        nextLocal = getNextValue(localKVIterator);
                    }
                }
            }
        }

    }

    private static KeyValue getNextValue(Iterator<KeyValue> iterator) {
        return iterator.hasNext() ? iterator.next() : null;
    }

    @AllArgsConstructor
    public static class KeyBytes {
        private final byte[] key;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyBytes keyBytes = (KeyBytes) o;
            return ByteArrayUtils.compare(this.key, keyBytes.key, 9, this.key.length - 2) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(key);
        }
    }
}
