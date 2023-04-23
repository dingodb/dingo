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

package io.dingodb.store.memory;

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.MetaService;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static io.dingodb.common.util.ByteArrayUtils.EMPTY_BYTES;
import static io.dingodb.common.util.ByteArrayUtils.MAX_BYTES;
import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class MemoryStoreInstance implements StoreInstance {
    private final NavigableMap<byte[], Object[]> db = new ConcurrentSkipListMap<>(ByteArrayUtils::compare);

    private DingoKeyValueCodec codec;

    public void initCodec(CommonId tableId) {
        codec = MetaService.root().getSubMetaService(MetaService.DINGO_NAME)
            .getTableDefinition(tableId)
            .createCodec();
    }

    @Override
    public Object[] getTupleByPrimaryKey(Object[] primaryKey) {
        try {
            return db.get(codec.encodeKey(primaryKey));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Object[]> getTuplesByPrimaryKeys(List<Object[]> primaryKeys) {
        try {
            List<Object[]> result = new ArrayList<>(primaryKeys.size());
            for (Object[] key : primaryKeys) {
                    result.add(db.get(codec.encodeKey(key)));
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Object[]> tupleScan() {
        return db.values().iterator();
    }

    @Override
    public Iterator<Object[]> tupleScan(
        Object[] start, Object[] end, boolean withStart, boolean withEnd
    ) {
        if (start == null && end == null) {
            return tupleScan();
        }
        withEnd = end != null && withEnd;
        byte[] startKey = Optional.mapOrGet(start, wrap(codec::encodeKey), db::firstKey);
        byte[] endKey = Optional.mapOrGet(end, wrap(codec::encodeKey), db::lastKey);

        return getTreeMapByRange(db.entrySet().iterator(), startKey, endKey, withStart, withEnd).values().iterator();
    }

    @Override
    public Iterator<KeyValue> keyValuePrefixScan(
        byte[] startPrimaryKey, byte[] endPrimaryKey, boolean includeStart, boolean includeEnd
    ) {
        return StoreInstance.super.keyValuePrefixScan(startPrimaryKey, endPrimaryKey, includeStart, includeEnd);
    }

    private TreeMap<byte[], Object[]> getTreeMapByRange(Iterator<Map.Entry<byte[], Object[]>> iterator,
                                                      byte[] startPrimaryKey,
                                                      byte[] endPrimaryKey,
                                                      boolean includeStart,
                                                      boolean includeEnd) {
        TreeMap<byte[], Object[]> treeMap = new TreeMap<>(ByteArrayUtils::compareWithoutLen);

        while (iterator.hasNext()) {
            Map.Entry<byte[], Object[]> next = iterator.next();
            boolean start = true;
            boolean end = false;
            if (includeStart) {
                start = ByteArrayUtils.greatThanOrEqual(next.getKey(), startPrimaryKey);
            } else {
                start = ByteArrayUtils.greatThan(next.getKey(), startPrimaryKey);
            }

            if (includeEnd) {
                end = ByteArrayUtils.lessThanOrEqual(next.getKey(), endPrimaryKey);
            } else {
                end = ByteArrayUtils.lessThan(next.getKey(), endPrimaryKey);
            }

            if (start && end) {
                treeMap.put(next.getKey(), next.getValue());
            }
        }
        return treeMap;
    }

    @Override
    public long countDeleteByRange(
        Object[] start, Object[] end, boolean withStart, boolean withEnd, boolean doDelete
    ) {
        if (start == null && end == null) {
            long count = db.size();
            if (doDelete) {
                db.clear();
            }
            return count;
        }
        withEnd = end == null || withEnd;
        byte[] startKey = Optional.mapOrGet(start, wrap(codec::encodeKey), db::firstKey);
        byte[] endKey = Optional.mapOrGet(end, wrap(codec::encodeKey), db::lastKey);
        Set<byte[]> keys = getTreeMapByRange(db.entrySet().iterator(), startKey, endKey, withStart, withEnd).keySet();
        if (doDelete) {
            keys.forEach(db::remove);
        }
        return keys.size();
    }

    @Override
    public boolean insert(Object[] row) {
        KeyValue keyValue = convertRow(row);
        db.put(keyValue.getKey(), row);
        return true;
    }

    @Override
    public boolean update(Object[] row) {
        KeyValue keyValue = convertRow(row);
        db.put(keyValue.getKey(), row);
        return true;
    }

    @Override
    public boolean delete(Object[] row) {
        KeyValue keyValue = convertRow(row);
        db.remove(keyValue.getKey());
        return true;
    }

    private KeyValue convertRow(Object[] row) {
        KeyValue keyValue = null;
        try {
            keyValue = codec.encode(row);
            return keyValue;
        } catch (IOException e) {
            log.error("Encode data error: {}", e.getMessage(), e);
        }
        return null;
    }
}
