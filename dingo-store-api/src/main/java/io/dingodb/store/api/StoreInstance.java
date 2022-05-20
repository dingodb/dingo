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
import io.dingodb.common.store.Part;
import io.dingodb.common.store.Row;

import java.util.Iterator;
import java.util.List;

public interface StoreInstance {

    default void assignPart(Part part) {
        throw new UnsupportedOperationException();
    }

    default void reassignPart(Part part) {
        throw new UnsupportedOperationException();
    }

    default void unassignPart(Part part) {
        throw new UnsupportedOperationException();
    }

    default void deletePart(Part part) {
        throw new UnsupportedOperationException();
    }

    default boolean exist(byte[] primaryKey) {
        throw new UnsupportedOperationException();
    }

    default boolean existAny(List<byte[]> primaryKeys) {
        throw new UnsupportedOperationException();
    }

    default boolean existAny(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        throw new UnsupportedOperationException();
    }

    default boolean upsert(Row row) {
        throw new UnsupportedOperationException();
    }

    default boolean upsert(List<Row> rows) {
        throw new UnsupportedOperationException();
    }

    default boolean upsertKeyValue(KeyValue row) {
        throw new UnsupportedOperationException();
    }

    default boolean upsertKeyValue(byte[] primaryKey, byte[] row) {
        throw new UnsupportedOperationException();
    }

    default boolean upsertKeyValue(List<KeyValue> rows) {
        throw new UnsupportedOperationException();
    }

    default boolean update(Row row) {
        throw new UnsupportedOperationException();
    }

    default boolean delete(byte[] primaryKey) {
        throw new UnsupportedOperationException();
    }

    default boolean delete(List<byte[]> primaryKeys) {
        throw new UnsupportedOperationException();
    }

    default boolean delete(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        throw new UnsupportedOperationException();
    }

    default void deleteByPrefixPrimaryKey(byte[] prefix) {
        byte[] end = new byte[prefix.length];
        System.arraycopy(prefix, 0, end, 0, prefix.length);
        ++end[end.length - 1];
        delete(prefix, end);
    }

    default Row getByPrimaryKey(byte[] primaryKey) {
        throw new UnsupportedOperationException();
    }

    default List<Row> getByPrimaryKeys(List<byte[]> primaryKey) {
        throw new UnsupportedOperationException();
    }

    default byte[] getValueByPrimaryKey(byte[] primaryKey) {
        throw new UnsupportedOperationException();
    }

    default KeyValue getKeyValueByPrimaryKey(byte[] primaryKey) {
        return new KeyValue(primaryKey, getValueByPrimaryKey(primaryKey));
    }

    default List<KeyValue> getKeyValueByPrimaryKeys(List<byte[]> primaryKeys) {
        throw new UnsupportedOperationException();
    }

    default Iterator<Row> scan() {
        throw new UnsupportedOperationException();
    }

    default Iterator<Row> scan(byte[] key) {
        byte[] stop = new byte[key.length];
        System.arraycopy(key, 0, stop, 0, key.length);
        stop[stop.length - 1]++;
        return scan(key, stop);
    }

    default Iterator<Row> scan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        throw new UnsupportedOperationException();
    }

    default Iterator<KeyValue> keyValueScan() {
        throw new UnsupportedOperationException();
    }

    default Iterator<KeyValue> keyValueScan(byte[] key) {
        byte[] stop = new byte[key.length];
        System.arraycopy(key, 0, stop, 0, key.length);
        stop[stop.length - 1]++;
        return keyValueScan(key, stop);
    }

    default Iterator<KeyValue> keyValueScan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        throw new UnsupportedOperationException();
    }

    default Iterator<byte[]> columnScan(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    default Iterator<byte[]> columnScan(int columnIndex, byte[] startPrimaryKey, byte[] endPrimaryKey) {
        throw new UnsupportedOperationException();
    }

}
