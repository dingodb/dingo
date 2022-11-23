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
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.store.api.StoreInstance;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
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
            return Iterators.transform(
                store.keyValueScan(),
                wrap(codec::decode, e -> log.error("Iterator: decode error.", e))::apply
            );
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
            // todo replace
            if (!prefixScan) {
                return Iterators.transform(
                    store.keyValueScan(startKey, endKey, includeStart, includeEnd),
                    wrap(codec::decode, e -> log.error("Iterator: decode error.", e))::apply
                );
            } else {
                includeEnd = true;
                return Iterators.transform(
                    store.keyValuePrefixScan(startKey, endKey, includeStart, includeEnd),
                    wrap(codec::decode, e -> log.error("Iterator: decode error.", e))::apply
                );
            }
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore getIterator cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public long countDeleteByRange(
        byte[] startPrimaryKey, byte[] endPrimaryKey, boolean includeStart, boolean includeEnd) {
        return store.countDeleteByRange(startPrimaryKey, endPrimaryKey, includeStart, includeEnd);
    }

    @Override
    public boolean insert(Object @NonNull [] tuple) {
        final long startTime = System.currentTimeMillis();
        try {
            KeyValue row = codec.encode(tuple);
            if (!store.exist(row.getPrimaryKey())) {
                store.upsertKeyValue(row);
                return true;
            }
        } catch (IOException e) {
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
            KeyValue row = codec.encode(tuple);
            store.upsertKeyValue(row);
        } catch (IOException e) {
            log.error("Upsert: encode error.", e);
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
            KeyValue row = codec.encode(tuple);
            return store.delete(row.getPrimaryKey());
        } catch (IOException e) {
            log.error("Remove: encode error.", e);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore remove cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
        return false;
    }

    @Override
    public long getEntryCntAndDeleteByPart(@NonNull List<String> startKeyList) {
        return getEntryCntOrDeleteByPart(startKeyList, true);
    }

    @Override
    public long getEntryCnt(@NonNull List<String> startKeyList) {
        return getEntryCntOrDeleteByPart(startKeyList, false);
    }

    @Override
    public Object @Nullable [] getByKey(Object @NonNull [] keyTuple) {
        final long startTime = System.currentTimeMillis();
        try {
            byte[] key = codec.encodeKey(keyTuple);
            byte[] value = store.getValueByPrimaryKey(key);
            if (value != null) {
                return codec.mapKeyAndDecodeValue(keyTuple, value);
            }
        } catch (IOException e) {
            log.error("GetByKey: codec error.", e);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore getByKey cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
        return null;
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
            List<KeyValue> valueList = store.getKeyValueByPrimaryKeys(keyList);
            if (keyList.size() != valueList.size()) {
                log.error("Get KeyValues from Store => keyCnt:{} mismatch valueCnt:{}",
                    keyList.size(),
                    valueList.size()
                );
            }
            ListIterator<Object[]> keyIt = keyTuples.listIterator();
            for (KeyValue row : valueList) {
                if (row == null) {
                    keyIt.next();
                    continue;
                }
                tuples.add(codec.mapKeyAndDecodeValue(keyIt.next(), row.getValue()));
            }
        } catch (IOException e) {
            log.error("Get KeyValues from Store => Catch Exception:{} when read data", e.getMessage(), e);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("PartInKvStore getByMultiKey cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
        return tuples;
    }

    private long getEntryCntOrDeleteByPart(@NonNull List<String> startKeyList, boolean doDeleting) {
        final long startTime = System.currentTimeMillis();
        long totalCnt = 0L;
        try {
            for (String partStartKey : startKeyList) {
                long currentCnt = 0;
                boolean isOK = false;
                byte[] partStartKeyInBytes = ByteArrayUtils.deCodeBase64String2Bytes(partStartKey);
                if (partStartKeyInBytes != null && partStartKeyInBytes.length > 0) {
                    isOK = true;
                    currentCnt = store.countOrDeletePart(partStartKeyInBytes, doDeleting);
                } else {
                    currentCnt = store.countOrDeletePart(ByteArrayUtils.EMPTY_BYTES, doDeleting);
                }
                totalCnt += currentCnt;
                log.info("count or delete table by part(Base64): {}, startBytes:{}, currentCnt:{}, AccumulatorCnt:{}"
                        + ", doDeleting: {}.",
                    partStartKey,
                    isOK ? Arrays.toString(partStartKeyInBytes) : "EMPTY_BYTES",
                    currentCnt,
                    totalCnt,
                    doDeleting);
            }
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("getEntryCntAndDeleteByPart total part:{} delete cost: {} ms.",
                    startKeyList.size(),
                    System.currentTimeMillis() - startTime);
            }
        }
        return totalCnt;
    }

    public Iterator<Object[]> keyValuePrefixScan(byte[] prefix) {
        return Iterators.transform(
            store.keyValuePrefixScan(prefix),
            wrap(codec::decode, e -> log.error("Iterator: decode error.", e))::apply
        );
    }
}
