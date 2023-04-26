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

    default long countDeleteByRange(
        Object[] startPrimaryKey, Object[] endPrimaryKey, boolean includeStart, boolean includeEnd, boolean doDelete
    ) {
        throw new UnsupportedOperationException();
    }

    default Object[] getTupleByPrimaryKey(Object[] primaryKey) {
        throw new UnsupportedOperationException();
    }

    default List<Object[]> getTuplesByPrimaryKeys(List<Object[]> primaryKeys) {
        throw new UnsupportedOperationException();
    }

    default Iterator<Object[]> tupleScan() {
        throw new UnsupportedOperationException();
    }

    default Iterator<Object[]> tupleScan(Object[] start, Object[] end, boolean withStart, boolean withEnd) {
        throw new UnsupportedOperationException();
    }

    default Iterator<Object[]> keyValuePrefixScan(Object[] prefix) {
        throw new UnsupportedOperationException();
    }

    default boolean insert(Object[] row) {
        throw new UnsupportedOperationException();
    }

    default boolean update(Object[] row) {
        throw new UnsupportedOperationException();
    }

    default boolean delete(Object[] row) {
        throw new UnsupportedOperationException();
    }

}
