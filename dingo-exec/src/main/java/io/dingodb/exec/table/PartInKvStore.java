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
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.Iterator;

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
            start = codec.resetKeyPrefix(start, store.id().domain);
            end = codec.resetKeyPrefix(end, store.id().domain);
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
        start = codec.resetKeyPrefix(start, store.id().domain);
        end = codec.resetKeyPrefix(end, store.id().domain);
        return Iterators.transform(store.scan(new StoreInstance.Range(start, end, withStart, withEnd), coprocessor),
            wrap(codec::decode)::apply
        );
    }

    @Override
    public long delete(byte[] start, byte[] end, boolean withStart, boolean withEnd) {
        final long startTime = System.currentTimeMillis();
        try {
            start = codec.resetKeyPrefix(start, store.id().domain);
            end = codec.resetKeyPrefix(end, store.id().domain);
            return store.delete(new StoreInstance.Range(start, end, withStart, withEnd));
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore insert cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public boolean insert(@NonNull Object[] keyValue) {
        try {
            return insert(codec.encode(keyValue));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean insert(@NonNull KeyValue keyValue) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.insert(codec.resetKeyPrefix(keyValue, store.id().domain));
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore insert cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public boolean update(@NonNull Object[] keyValue) {
        try {
            return update(codec.encode(keyValue));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean update(@NonNull KeyValue keyValue) {
        return update(keyValue, new KeyValue(keyValue.getKey(), null));
    }

    @Override
    public boolean update(@NonNull Object[] newTuple, @NonNull Object[] oldTuple) {
        try {
            return update(codec.encode(newTuple), codec.encode(oldTuple));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        try {
            return remove(codec.encode(tuple).getKey());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean remove(byte @NonNull [] key) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.delete(codec.resetKeyPrefix(key, store.id().domain));
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
            start = codec.resetKeyPrefix(start, store.id().domain);
            end = codec.resetKeyPrefix(end, store.id().domain);
            return store.count(new StoreInstance.Range(start, end, withStart, withEnd));
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore insert cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public Object @Nullable [] get(Object @NonNull [] key) {
        try {
            return get(codec.encodeKey(key));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object @Nullable [] get(byte @NonNull [] key) {
        final long startTime = System.currentTimeMillis();
        try {
            return Optional.mapOrNull(store.get(codec.resetKeyPrefix(key, store.id().domain)), wrap(codec::decode));
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore insert cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

}
