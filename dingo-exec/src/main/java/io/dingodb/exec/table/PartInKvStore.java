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
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.store.api.StoreInstance;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public final class PartInKvStore implements Part {
    private final StoreInstance store;
    @Getter
    private final KeyValueCodec codec;

    public PartInKvStore(StoreInstance store, DingoType schema, TupleMapping keyMapping) {
        this.store = store;
        this.codec = new DingoKeyValueCodec(schema, keyMapping);
    }

    @Override
    public @NonNull Iterator<Object[]> getIterator() {
        final long startTime = System.currentTimeMillis();
        try {
            return store.tupleScan();
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore getIterator cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public @NonNull Iterator<Object[]> getIteratorByRange(
        byte[] startKey, byte[] endKey, boolean includeStart, boolean includeEnd, boolean prefixScan
    ) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.tupleScan(
                startKey == null ? null : codec.decodeKey(startKey),
                endKey == null ? null: codec.decodeKey(endKey),
                includeStart,
                includeEnd
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore getIterator cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public long countDeleteByRange(
        byte[] startPrimaryKey, byte[] endPrimaryKey, boolean includeStart, boolean includeEnd
    ) {
        return countOrDeleteByRange(startPrimaryKey, endPrimaryKey, includeStart, includeEnd, true);
    }

    public long countOrDeleteByRange(
        byte[] start, byte[] end, boolean includeStart, boolean includeEnd, boolean doDelete
    ) {
        try {
            return store.countDeleteByRange(
                start == null || start.length == 0 ? null : codec.decodeKey(start),
                end == null ? null : codec.decodeKey(end),
                includeStart,
                includeEnd,
                doDelete
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean insert(Object @NonNull [] tuple) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.insert(tuple);
        } catch (Exception e) {
            log.error("Insert: encode error.", e);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore insert cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
        return false;
    }

    @Override
    public void upsert(Object @NonNull [] tuple) {
        final long startTime = System.currentTimeMillis();
        try {
            store.update(tuple);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore upsert cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public boolean remove(Object @NonNull [] tuple) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.delete(tuple);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore remove cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public long getEntryCntAndDeleteByPart() {
        return countOrDeleteByRange(null, null, true, false, true);
    }

    @Override
    public long getEntryCnt(byte[] startKey, byte[] endKey, boolean includeStart, boolean includeEnd) {
        return countOrDeleteByRange(startKey, endKey, includeStart, includeEnd, false);
    }

    @Override
    public Object @Nullable [] getByKey(Object @NonNull [] keyTuple) {
        final long startTime = System.currentTimeMillis();
        try {
            return store.getTupleByPrimaryKey(keyTuple);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore getByKey cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public @NonNull List<Object[]> getByMultiKey(final @NonNull List<Object[]> keyTuples) {
        List<byte[]> keyList = keyTuples.stream()
            .map(wrap(codec::encodeKey, e -> log.error("GetByMultiKey: encode key error.", e)))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        List<Object[]> tuples = new ArrayList<>(keyList.size());
        final long startTime = System.currentTimeMillis();
        try {
            List<Object[]> valueList = store.getTuplesByPrimaryKeys(keyTuples);
            if (keyList.size() != valueList.size()) {
                log.error("Get KeyValues from Store => keyCnt:{} mismatch valueCnt:{}",
                    keyList.size(),
                    valueList.size()
                );
            }
            return valueList;
        } catch (Exception e) {
            log.error("Get KeyValues from Store => Catch Exception:{} when read data", e.getMessage(), e);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore getByMultiKey cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
        return tuples;
    }

    public Iterator<Object[]> keyValuePrefixScan(byte[] prefix) {
        return Iterators.transform(
            store.keyValuePrefixScan(prefix),
            wrap(codec::decode, e -> log.error("Iterator: decode error.", e))::apply
        );
    }
}
