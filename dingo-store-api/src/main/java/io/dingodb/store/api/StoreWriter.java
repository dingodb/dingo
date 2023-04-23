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

import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Row;

import java.util.List;

public interface StoreWriter {

    default boolean insert(Row row) {
        throw new UnsupportedOperationException();
    }

    default long insert(List<Row> rows) {
        throw new UnsupportedOperationException();
    }

    default boolean insertKeyValue(KeyValue row) {
        throw new UnsupportedOperationException();
    }

    default boolean insertKeyValue(byte[] primaryKey, byte[] row) {
        throw new UnsupportedOperationException();
    }

    default long insertKeyValue(List<KeyValue> rows) {
        throw new UnsupportedOperationException();
    }

    default long update(Row oldRow, Row newRow) {
        throw new UnsupportedOperationException();
    }

    default long update(List<Row> oldRows, List<Row> newRows) {
        throw new UnsupportedOperationException();
    }

    default boolean updateKeyValue(KeyValue row) {
        throw new UnsupportedOperationException();
    }

    default boolean updateKeyValue(byte[] primaryKey, byte[] value) {
        throw new UnsupportedOperationException();
    }

    default long updateKeyValue(List<KeyValue> oldRows, List<KeyValue> oldValue) {
        throw new UnsupportedOperationException();
    }

    default boolean delete(byte[] primaryKey) {
        throw new UnsupportedOperationException();
    }

    default long delete(List<byte[]> primaryKeys) {
        throw new UnsupportedOperationException();
    }

    default long delete(byte[] begin, byte[] end) {
        return delete(begin, end, true, false);
    }

    default long delete(byte[] begin, byte[] end, boolean withBegin, boolean withEnd) {
        throw new UnsupportedOperationException();
    }

}
