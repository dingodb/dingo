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

package io.dingodb.store.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.store.KeyValue;
import lombok.AllArgsConstructor;

import java.util.Iterator;
import java.util.List;

public interface StoreInstance {

    @AllArgsConstructor
    class Range {
        public final byte[] start;
        public final byte[] end;
        public final boolean withStart;
        public final boolean withEnd;
    }

    default CommonId id() {
        throw new UnsupportedOperationException();
    }

    default boolean insert(KeyValue row) {
        throw new UnsupportedOperationException();
    }

    default boolean update(KeyValue row, KeyValue old) {
        throw new UnsupportedOperationException();
    }

    default boolean delete(byte[] key) {
        throw new UnsupportedOperationException();
    }

    default long delete(Range range) {
        throw new UnsupportedOperationException();
    }

    default KeyValue get(byte[] key) {
        throw new UnsupportedOperationException();
    }

    default List<KeyValue> get(List<byte[]> keys) {
        throw new UnsupportedOperationException();
    }

    default Iterator<KeyValue> scan(byte[] prefix) {
        return scan(new Range(prefix, prefix, true, true));
    }

    default Iterator<KeyValue> scan(Range range) {
        throw new UnsupportedOperationException();
    }

    default Iterator<KeyValue> scan(Range range, Coprocessor coprocessor) {
        throw new UnsupportedOperationException();
    }

    default long count(Range range) {
        throw new UnsupportedOperationException();
    }

    default boolean insertWithIndex(Object[] record) {
        throw new UnsupportedOperationException();
    }

    default boolean insertIndex(Object[] record) {
        throw new UnsupportedOperationException();
    }

    default boolean deleteWithIndex(Object[] key) {
        throw new UnsupportedOperationException();
    }

    default boolean deleteIndex(Object[] key) {
        throw new UnsupportedOperationException();
    }
}
