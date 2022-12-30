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
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.common.store.Row;
import io.dingodb.common.util.ByteArrayUtils;

import java.util.Iterator;
import java.util.List;

public interface StoreInstance {

    // todo A temporary solution need refactor report stats
    @Deprecated
    default void openReportStats(CommonId part) {
        throw new UnsupportedOperationException();
    }

    // todo A temporary solution need refactor report stats
    @Deprecated
    default void closeReportStats(CommonId part) {
        throw new UnsupportedOperationException();
    }

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

    default long countOrDeletePart(byte[] startKey, boolean doDeleting) {
        throw new UnsupportedOperationException();
    }

    default long countDeleteByRange(
        byte[] startPrimaryKey, byte[] endPrimaryKey, boolean includeStart, boolean includeEnd) {
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
        return upsertKeyValue(row.getKey(), row.getValue());
    }

    default boolean upsertKeyValue(byte[] primaryKey, byte[] row) {
        throw new UnsupportedOperationException();
    }

    default boolean upsertKeyValue(List<KeyValue> rows) {
        rows.forEach(this::upsertKeyValue);
        return true;
    }

    default boolean update(Row row) {
        throw new UnsupportedOperationException();
    }

    default Object compute(List<byte[]> startPrimaryKey, List<byte[]> endPrimaryKey, byte[] op, boolean readOnly) {
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
        byte[] stopInBytes = ByteArrayUtils.increment(prefix);
        delete(prefix, stopInBytes);
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
        byte[] stopInBytes = ByteArrayUtils.increment(key);
        return scan(key, stopInBytes);
    }

    default Iterator<Row> scan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        throw new UnsupportedOperationException();
    }

    default Iterator<KeyValue> keyValueScan() {
        throw new UnsupportedOperationException();
    }

    default Iterator<KeyValue> keyValueScan(byte[] key) {
        return keyValueScan(key, ByteArrayUtils.increment(key));
    }

    default Iterator<KeyValue> keyValueScan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        return keyValueScan(startPrimaryKey, endPrimaryKey, true, false);
    }

    default Iterator<KeyValue> keyValueScan(
        byte[] startPrimaryKey, byte[] endPrimaryKey, boolean includeStart, boolean includeEnd
    ) {
        throw new UnsupportedOperationException();
    }

    default Iterator<KeyValue> keyValuePrefixScan(
        byte[] startPrimaryKey, byte[] endPrimaryKey, boolean includeStart, boolean includeEnd
    ) {
        throw new UnsupportedOperationException();
    }

    default Iterator<KeyValue> keyValuePrefixScan(byte[] prefix) {
        throw new UnsupportedOperationException();
    }

    default Iterator<byte[]> columnScan(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    default Iterator<byte[]> columnScan(int columnIndex, byte[] startPrimaryKey, byte[] endPrimaryKey) {
        throw new UnsupportedOperationException();
    }

    default KeyValue udfGet(byte[] primaryKey, String udfName, String functionName, int version) {
        throw new UnsupportedOperationException();
    }

    default boolean udfUpdate(byte[] primaryKey, String udfName, String functionName, int version) {
        throw new UnsupportedOperationException();
    }
}
