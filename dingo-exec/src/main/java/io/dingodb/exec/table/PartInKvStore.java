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

import com.google.common.collect.Iterators;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Optional;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.concurrent.Future;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public final class PartInKvStore implements Part {
    private final StoreInstance store;
    final KeyValueCodec codec;

    public PartInKvStore(StoreInstance store, KeyValueCodec codec) {
        this.store = store;
        this.codec = codec;
    }

    @Override
    public @NonNull Iterator<Object[]> scan(byte[] start, byte[] end, boolean withStart, boolean withEnd) {
        final long startTime = System.currentTimeMillis();
        try {
            return Iterators.transform(store.scan(new StoreInstance.Range(start, end, withStart, withEnd)),
                wrap(codec::decode)::apply
            );
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore insert cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public @NonNull Iterator<Object[]> scan(
        byte[] start,
        byte[] end,
        boolean withStart,
        boolean withEnd,
        Coprocessor coprocessor
    ) {
        return Iterators.transform(store.scan(new StoreInstance.Range(start, end, withStart, withEnd), coprocessor),
            wrap(codec::decode)::apply
        );
    }

    @Override
    public long delete(byte[] start, byte[] end, boolean withStart, boolean withEnd) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.delete(new StoreInstance.Range(start, end, withStart, withEnd));
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore insert cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public boolean insert(@NonNull Object[] keyValue) {
        boolean insert = store.insertIndex(keyValue);
        if (insert) {
            return store.insertWithIndex(keyValue);
        }

        return false;
    }

    @Override
    public boolean insert(@NonNull KeyValue keyValue) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.insert(keyValue);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore insert cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public boolean update(@NonNull Object[] keyValue) {
        return update(codec.encode(keyValue));
    }

    @Override
    public boolean update(@NonNull KeyValue keyValue) {
        return update(keyValue, new KeyValue(keyValue.getKey(), null));
    }

    @Override
    public boolean update(@NonNull Object[] newTuple, @NonNull Object[] oldTuple) {
        if (store.insertIndex(newTuple)) {
            if (store.updateWithIndex(newTuple, oldTuple)) {
                store.deleteIndex(newTuple, oldTuple);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean update(@NonNull KeyValue newKeyValue, @NonNull KeyValue oldKeyValue) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.update(newKeyValue, oldKeyValue);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore update cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public boolean remove(@NonNull Object[] tuple) {
        if (store.deleteWithIndex(tuple)) {
            return store.deleteIndex(tuple);
        }
        return false;
    }

    @Override
    public boolean remove(byte @NonNull [] key) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.delete(key);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore insert cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public long count(byte[] start, byte[] end, boolean withStart, boolean withEnd) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.count(new StoreInstance.Range(start, end, withStart, withEnd));
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore insert cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public Object @Nullable [] get(Object @NonNull [] key) {
        return get(codec.encodeKey(key));
    }

    @Override
    public Object @Nullable [] get(byte @NonNull [] key) {
        final long startTime = System.currentTimeMillis();
        try {
            return Optional.mapOrNull(store.get(key), wrap(codec::decode));
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore insert cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    public boolean txnPreWrite(@NonNull TxnPreWrite txnPreWrite, long timeOut) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.txnPreWrite(txnPreWrite, timeOut);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore txnPreWrite cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public Future txnPreWritePrimaryKey(@NonNull TxnPreWrite txnPreWrite, long timeOut) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.txnPreWritePrimaryKey(txnPreWrite, timeOut);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore txnPreWritePrimaryKey cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public boolean txnCommit(@NonNull TxnCommit commitRequest) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.txnCommit(commitRequest);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore txnCommit cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public boolean txnBatchRollBack(@NonNull TxnBatchRollBack rollBackRequest) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.txnBatchRollback(rollBackRequest);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore txnBatchRollback cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

}
