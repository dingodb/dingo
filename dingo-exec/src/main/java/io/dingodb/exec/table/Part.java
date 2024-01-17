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

package io.dingodb.exec.table;

import io.dingodb.common.Coprocessor;
import io.dingodb.common.store.KeyValue;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public interface Part {

    default @NonNull Iterator<Object[]> scan(byte[] prefix) {
        return scan(prefix, prefix, true, true);
    }

    @NonNull Iterator<Object[]> scan(byte[] start, byte[] end, boolean withStart, boolean withEnd);

    @NonNull Iterator<Object[]> scan(
        byte[] start,
        byte[] end,
        boolean withStart,
        boolean withEnd,
        Coprocessor coprocessor
    );

    long delete(byte[] start, byte[] end, boolean withStart, boolean withEnd);

    boolean insert(@NonNull KeyValue keyValue);

    boolean insert(@NonNull Object[] keyValue);

    boolean update(@NonNull KeyValue keyValue);

    boolean update(@NonNull KeyValue newKeyValue, @NonNull KeyValue oldKeyValue);

    boolean update(@NonNull Object[] keyValue);

    boolean update(@NonNull Object[] newTuple, @NonNull Object[] oldTuple);

    boolean remove(byte @NonNull [] key);

    boolean remove(@NonNull Object[] key);

    long count(byte[] start, byte[] end, boolean withStart, boolean withEnd);

    Object @Nullable [] get(byte @NonNull [] key);

    Object @Nullable [] get(Object @NonNull [] key);

    default @NonNull List<Object[]> get(final @NonNull List<byte[]> keys) {
        return keys.stream()
            .map(this::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    public boolean txnPreWrite(@NonNull TxnPreWrite txnPreWrite, long timeOut);

    public Future txnPreWritePrimaryKey(@NonNull TxnPreWrite txnPreWrite, long timeOut);

    public boolean txnCommit(@NonNull TxnCommit commitRequest);

    public boolean txnBatchRollBack(@NonNull TxnBatchRollBack rollBackRequest);
}
