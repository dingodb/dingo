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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

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
    public void deletePart(@Nonnull Part part) {
        db.subMap(part.getStart(), true, part.getEnd(), false).keySet().forEach(db::remove);
        startKeyPartMap.remove(part.getStart());
    }

    @Override
    public long countOrDeletePart(byte[] startKey, boolean doDeleting) {
        Part part = startKeyPartMap.get(startKey);
        byte[] startKeyInBytes = part.getStart() == null ? EMPTY_BYTES : part.getStart();
        byte[] endKeyInBytes = part.getEnd() == null ? MAX_BYTES : part.getEnd();
        Map<byte[], byte[]> subMap = db.subMap(startKeyInBytes, true, endKeyInBytes, false);
        long count = subMap.size();
        if (doDeleting) {
            subMap.clear();
        }
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
        return db.subMap(startPrimaryKey, true, endPrimaryKey, false).size() > 0;
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
        db.subMap(startPrimaryKey, true, endPrimaryKey, false).keySet().forEach(db::remove);
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
        return new KeyValueIterator(
            new TreeMap<>(db.subMap(startPrimaryKey, true, endPrimaryKey, false)).entrySet().iterator()
        );
    }
}
