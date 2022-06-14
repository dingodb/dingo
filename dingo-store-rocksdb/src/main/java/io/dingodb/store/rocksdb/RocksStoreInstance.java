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

package io.dingodb.store.rocksdb;

import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.common.util.Files;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class RocksStoreInstance implements StoreInstance {
    private final Path path;
    private final RocksDB db;
    private final WriteOptions writeOptions;

    public RocksStoreInstance(CommonId id) {
        try {
            this.path = Paths.get(RocksConfiguration.dataPath(), id.toString());
            Files.createDirectories(path);
            this.db = RocksDB.open(path.toString());
            this.writeOptions = new WriteOptions();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void assignPart(Part part) {

    }

    @Override
    public void reassignPart(Part part) {

    }

    @Override
    public void unassignPart(Part part) {
        deletePart(part);
    }

    @Override
    public void deletePart(Part part) {
        try {
            db.deleteRange(part.getStart(), part.getEnd());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long countOrDeletePart(byte[] startKey, boolean doDeleting) {
        long count = 0;
        List<byte[]> keyList = new ArrayList<>(1024);
        try (ReadOptions readOptions = new ReadOptions()) {
            try (Snapshot snapshot = this.db.getSnapshot()) {
                readOptions.setSnapshot(snapshot);
                RocksIterator rocksIterator = db.newIterator(readOptions);
                while (rocksIterator.isValid()) {
                    count++;
                    keyList.add(rocksIterator.key());
                    rocksIterator.next();
                }
            }
        }
        if (!doDeleting) {
            keyList.stream().forEach(x -> {
                try {
                    db.delete(x);
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return count;
    }

    @Override
    public boolean exist(byte[] primaryKey) {
        try {
            return db.get(primaryKey) != null;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
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
        return new RocksBlockIterator(db, startPrimaryKey, endPrimaryKey).hasNext();
    }

    @Override
    public boolean upsertKeyValue(KeyValue row) {
        try {
            db.put(writeOptions, row.getKey(), row.getValue());
            return true;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean upsertKeyValue(byte[] primaryKey, byte[] row) {
        try {
            db.put(writeOptions, primaryKey, row);
            return true;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean upsertKeyValue(List<KeyValue> rows) {
        try (final WriteBatch batch = new WriteBatch()) {
            for (KeyValue keyValue : rows) {
                batch.put(keyValue.getKey(), keyValue.getValue());
            }
            this.db.write(writeOptions, batch);
            return true;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean delete(byte[] primaryKey) {
        try {
            db.delete(primaryKey);
            return true;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean delete(List<byte[]> primaryKeys) {
        try (final WriteBatch batch = new WriteBatch()) {
            for (final byte[] key : primaryKeys) {
                batch.delete(key);
            }
            this.db.write(this.writeOptions, batch);
            return true;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean delete(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        try {
            this.db.deleteRange(this.writeOptions, startPrimaryKey, endPrimaryKey);
            return true;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public byte[] getValueByPrimaryKey(byte[] primaryKey) {
        try {
            return db.get(primaryKey);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public KeyValue getKeyValueByPrimaryKey(byte[] primaryKey) {
        try {
            return new KeyValue(primaryKey, db.get(primaryKey));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(List<byte[]> primaryKeys) {
        try {
            List<byte[]> values = db.multiGetAsList(primaryKeys);
            List<KeyValue> entries = new ArrayList<>(primaryKeys.size());
            for (int i = 0; i < primaryKeys.size(); i++) {
                entries.add(new KeyValue(primaryKeys.get(i), values.get(i)));
            }
            return entries;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<KeyValue> keyValueScan() {
        return new RocksBlockIterator(db, null, null);
    }

    @Override
    public Iterator<KeyValue> keyValueScan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        return new RocksBlockIterator(db, startPrimaryKey, endPrimaryKey);
    }
}
