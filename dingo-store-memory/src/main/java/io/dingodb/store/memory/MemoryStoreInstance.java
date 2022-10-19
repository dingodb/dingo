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

import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.store.api.StoreInstance;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static io.dingodb.common.util.ByteArrayUtils.EMPTY_BYTES;
import static io.dingodb.common.util.ByteArrayUtils.MAX_BYTES;

public class MemoryStoreInstance implements StoreInstance {
    private final Map<byte[], Part> startKeyPartMap = new ConcurrentSkipListMap<>(ByteArrayUtils::compare);
    private final NavigableMap<byte[], byte[]> db = new ConcurrentSkipListMap<>(ByteArrayUtils::compare);

    @Override
    public void assignPart(Part part) {
        startKeyPartMap.put(part.getStart(), part);
    }

    @Override
    public void unassignPart(Part part) {
        deletePart(part);
    }

    @Override
    public void deletePart(@NonNull Part part) {
        TreeMap<byte[], byte[]> treeMap = getTreeMapByRange(
            db.entrySet().iterator(), part.getStart(), part.getEnd(), true, false);
        treeMap.keySet().forEach(db::remove);
        startKeyPartMap.remove(part.getStart());
    }

    @Override
    public long countOrDeletePart(byte[] startKey, boolean doDeleting) {
        Part part = startKeyPartMap.get(startKey);
        byte[] startKeyInBytes = part.getStart() == null ? EMPTY_BYTES : part.getStart();
        byte[] endKeyInBytes = part.getEnd() == null ? MAX_BYTES : part.getEnd();

        TreeMap<byte[], byte[]> subMap = getTreeMapByRange(
            db.entrySet().iterator(), startKeyInBytes, endKeyInBytes, true, false);
        long count = subMap.size();
        if (doDeleting) {
            delete(part.getStart(), part.getEnd());
        }
        return count;
    }

    @Override
    public long countDeleteByRange(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        if (endPrimaryKey == null) {
            endPrimaryKey = db.lastKey();
        }
        TreeMap<byte[], byte[]> subMap = getTreeMapByRange(
            db.entrySet().iterator(), startPrimaryKey, endPrimaryKey, true, false);
        long count = subMap.size();
        delete(startPrimaryKey, endPrimaryKey);

        return count;
    }

    @Override
    public boolean exist(byte[] primaryKey) {
        return db.containsKey(primaryKey);
    }

    @Override
    public boolean existAny(List<byte[]> primaryKeys) {
        for (byte[] primaryKey : primaryKeys) {
            if (exist(primaryKey)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean existAny(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        TreeMap<byte[], byte[]> treeMap = getTreeMapByRange(
            db.entrySet().iterator(), startPrimaryKey, endPrimaryKey, true, false);
        return treeMap.size() > 0;
    }

    @Override
    public boolean upsertKeyValue(KeyValue row) {
        db.put(row.getKey(), row.getValue());
        return true;
    }

    @Override
    public boolean upsertKeyValue(byte[] primaryKey, byte[] row) {
        db.put(primaryKey, row);
        return true;
    }

    @Override
    public boolean upsertKeyValue(List<KeyValue> rows) {
        rows.forEach(row -> db.put(row.getKey(), row.getValue()));
        return true;
    }

    @Override
    public boolean delete(byte[] primaryKey) {
        db.remove(primaryKey);
        return true;
    }

    @Override
    public boolean delete(List<byte[]> primaryKeys) {
        primaryKeys.forEach(db::remove);
        return true;
    }

    @Override
    public boolean delete(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        TreeMap<byte[], byte[]> treeMap = getTreeMapByRange(
            db.entrySet().iterator(), startPrimaryKey, endPrimaryKey, true, false);
        treeMap.keySet().forEach(db::remove);
        return true;
    }

    @Override
    public byte[] getValueByPrimaryKey(byte[] primaryKey) {
        return db.get(primaryKey);
    }

    @Override
    public KeyValue getKeyValueByPrimaryKey(byte[] primaryKey) {
        return new KeyValue(primaryKey, db.get(primaryKey));
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(List<byte[]> primaryKeys) {
        return primaryKeys.stream().map(key -> new KeyValue(key, db.get(key))).collect(Collectors.toList());
    }

    @Override
    public Iterator<KeyValue> keyValueScan() {
        return new KeyValueIterator(new TreeMap<>(db).entrySet().iterator());
    }

    @Override
    public Iterator<KeyValue> keyValueScan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        if (endPrimaryKey == null) {
            endPrimaryKey = db.lastKey();
        }

        TreeMap<byte[], byte[]> treeMap = getTreeMapByRange(
            db.entrySet().iterator(), startPrimaryKey, endPrimaryKey, true, false);
        return new KeyValueIterator(treeMap.entrySet().iterator()
        );
    }

    @Override
    public Iterator<KeyValue> keyValueScan(
        byte[] startPrimaryKey, byte[] endPrimaryKey, boolean includeStart, boolean includeEnd
    ) {
        if (endPrimaryKey == null) {
            endPrimaryKey = db.lastKey();
        }

        TreeMap<byte[], byte[]> treeMap = getTreeMapByRange(
            db.entrySet().iterator(), startPrimaryKey, endPrimaryKey, includeStart, includeEnd);
        return new KeyValueIterator(treeMap.entrySet().iterator());
    }

    private TreeMap<byte[], byte[]> getTreeMapByRange(Iterator<Map.Entry<byte[], byte[]>> iterator,
                                                      byte[] startPrimaryKey,
                                                      byte[] endPrimaryKey,
                                                      boolean includeStart,
                                                      boolean includeEnd) {
        TreeMap<byte[], byte[]> treeMap = new TreeMap<>(ByteArrayUtils::compareContainsEnd);

        while (iterator.hasNext()) {
            Map.Entry<byte[], byte[]> next = iterator.next();
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
}
