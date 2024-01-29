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
import io.dingodb.common.CoprocessorV2;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.vector.VectorSearchResponse;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import io.dingodb.store.api.transaction.data.rollback.TxnPessimisticRollBack;
import lombok.AllArgsConstructor;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

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

    @Deprecated
    default boolean insert(KeyValue row) {
        return insert(System.identityHashCode(row), row);
    }

    default boolean insert(long requestTs, KeyValue row) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default boolean update(KeyValue row, KeyValue old) {
        return update(System.identityHashCode(row), row, old);
    }

    default boolean update(long requestTs, KeyValue row, KeyValue old) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default boolean put(KeyValue row) {
        return put(System.identityHashCode(row), row);
    }

    default boolean put(long requestTs, KeyValue row) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default boolean delete(byte[] key) {
        return delete(System.identityHashCode(key), key);
    }

    default boolean delete(long requestTs, byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default long delete(Range range) {
        return delete(System.identityHashCode(range), range);
    }

    default long delete(long requestTs, Range range) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default void deletePrefix(byte[] prefix) {
        deletePrefix(System.identityHashCode(prefix), prefix);
    }

    default void deletePrefix(long requestTs, byte[] prefix) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default KeyValue get(byte[] key) {
        return get(System.identityHashCode(key), key);
    }

    default KeyValue get(long requestTs, byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default List<KeyValue> get(List<byte[]> keys) {
        return get(System.identityHashCode(keys), keys);
    }

    default List<KeyValue> get(long requestId, List<byte[]> keys) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default Iterator<KeyValue> scan(byte[] prefix) {
        return scan(System.identityHashCode(prefix), prefix);
    }

    default Iterator<KeyValue> scan(long requestTs, byte[] prefix) {
        return scan(new Range(prefix, prefix, true, true));
    }

    @Deprecated
    default Iterator<KeyValue> scan(Range range) {
        return scan(System.identityHashCode(range), range);
    }

    default Iterator<KeyValue> scan(long requestTs, Range range) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default Iterator<KeyValue> scan(Range range, Coprocessor coprocessor) {
        return scan(System.identityHashCode(range), range, coprocessor);
    }

    default Iterator<KeyValue> scan(long requestTs, Range range, Coprocessor coprocessor) {
        if (coprocessor == null) {
            return scan(requestTs, range);
        }
        throw new UnsupportedOperationException();
    }

    default Iterator<KeyValue> scan(long requestTs, Range range, CoprocessorV2 coprocessor) {
        if (coprocessor == null) {
            return scan(requestTs, range);
        }
        throw new UnsupportedOperationException();
    }

    default List<VectorSearchResponse> vectorSearch(
        CommonId indexId, Float[] floatArray, int topN, Map<String, Object> parameterMap
    ) {
        return vectorSearch(System.identityHashCode(floatArray), indexId, floatArray, topN, parameterMap, null);
    }

    default List<VectorSearchResponse> vectorSearch(
        long requestTs, CommonId indexId, Float[] floatArray, int topN, Map<String, Object> parameterMap, CoprocessorV2 coprocessorV2
    ) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default long count(Range range) {
        throw new UnsupportedOperationException();
    }

    default long count(long requestTs, Range range) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default boolean insertWithIndex(Object[] record) {
        return insertWithIndex(System.identityHashCode(record), record);
    }

    default boolean insertWithIndex(long requestTs, Object[] record) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default boolean insertIndex(Object[] record) {
        return insertIndex(System.identityHashCode(record), record);
    }

    default boolean insertIndex(long requestTs, Object[] record) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default boolean updateWithIndex(Object[] newRecord, Object[] oldRecord) {
        return updateWithIndex(System.identityHashCode(newRecord), newRecord, oldRecord);
    }

    default boolean updateWithIndex(long requestTs, Object[] newRecord, Object[] oldRecord) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default boolean deleteWithIndex(Object[] key) {
        return deleteWithIndex(System.identityHashCode(key), key);
    }

    default boolean deleteWithIndex(long requestTs, Object[] key) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default boolean deleteIndex(Object[] key) {
        return deleteIndex(System.identityHashCode(key), key);
    }

    default boolean deleteIndex(long requestId, Object[] key) {
        throw new UnsupportedOperationException();
    }

    // TODO Temporary way, to be optimized
    @Deprecated
    default boolean deleteIndex(Object[] newRecord, Object[] oldRecord) {
        return deleteIndex(System.identityHashCode(newRecord), newRecord, oldRecord);
    }

    default boolean deleteIndex(long requestTs, Object[] newRecord, Object[] oldRecord) {
        throw new UnsupportedOperationException();
    }

    default Iterator<KeyValue> txnScan(long startTs, Range range, long timeOut) {
        throw new UnsupportedOperationException();
    }

    default Iterator<KeyValue> txnScan(long startTs, Range range, long timeOut, CoprocessorV2 coprocessor) {
        if (coprocessor == null) {
            return txnScan(startTs, range, timeOut);
        }
        throw new UnsupportedOperationException();
    }

    default KeyValue txnGet(long startTs, byte[] key, long timeOut) {
        return txnGet(startTs, Collections.singletonList(key), timeOut).get(0);
    }

    default List<KeyValue> txnGet(long startTs, List<byte[]> keys, long timeOut) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default boolean txnPreWrite(TxnPreWrite txnPreWrite, long timeOut) {
        return txnPreWrite(txnPreWrite.getStartTs(), txnPreWrite, timeOut);
    }

    default boolean txnPreWrite(long requestTs, TxnPreWrite txnPreWrite, long timeOut) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default Future txnPreWritePrimaryKey(TxnPreWrite txnPreWrite, long timeOut) {
        return txnPreWritePrimaryKey(System.identityHashCode(txnPreWrite), txnPreWrite, timeOut);
    }

    default Future txnPreWritePrimaryKey(long requestTs, TxnPreWrite txnPreWrite, long timeOut) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default boolean txnCommit(TxnCommit txnCommit) {
        throw new UnsupportedOperationException();
    }

    default boolean txnCommit(long requestTs, TxnCommit txnCommit) {
        throw new UnsupportedOperationException();
    }

    default Future txnPessimisticLockPrimaryKey(TxnPessimisticLock txnPessimisticLock, long timeOut) {
        throw new UnsupportedOperationException();
    }

    default boolean txnPessimisticLock(TxnPessimisticLock txnPessimisticLock, long timeOut) {
        throw new UnsupportedOperationException();
    }

    default boolean txnPessimisticLockRollback(TxnPessimisticRollBack txnPessimisticRollBack) {
        throw new UnsupportedOperationException();
    }

    default boolean txnBatchRollback(TxnBatchRollBack txnBatchRollBack) {
        throw new UnsupportedOperationException();
    }

    default boolean txnBatchRollback(long requestTs, TxnBatchRollBack txnBatchRollBack) {
        throw new UnsupportedOperationException();
    }
}
