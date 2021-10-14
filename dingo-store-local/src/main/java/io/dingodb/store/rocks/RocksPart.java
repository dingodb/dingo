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

package io.dingodb.store.rocks;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.store.TablePart;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Slf4j
public final class RocksPart implements TablePart {
    private final String path;
    @Getter
    private final RocksKeyValueCodec codec;

    private RocksDB dbReadOnly;
    private RocksDB dbWritable;

    public RocksPart(String path, TupleSchema schema, TupleMapping keyMapping) {
        this.path = path;
        this.codec = new RocksKeyValueCodec(schema, keyMapping);
        create();
        tryOpenWritable();
    }

    @Nonnull
    private String dataDir() {
        return path + File.separator + "_data";
    }

    private void tryOpenReadOnly() {
        if (dbReadOnly != null) {
            return;
        }
        try {
            // TODO: where to close it?
            dbReadOnly = RocksDB.openReadOnly(dataDir());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private void tryOpenWritable() {
        if (dbWritable != null) {
            return;
        }
        try (Options options = new Options().setCreateIfMissing(true)) {
            // TODO: where to close it?
            dbWritable = RocksDB.open(options, dataDir());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void create() {
        try {
            FileUtils.forceMkdir(new File(path));
        } catch (IOException e) {
            throw new RuntimeException("Cannot create dir.", e);
        }
    }

    @Override
    @Nonnull
    public Iterator<Object[]> getIterator() {
        tryOpenReadOnly();
        return Iterators.transform(new RocksDbIterator(dbReadOnly), new Function<RocksKeyValue, Object[]>() {
            @Nullable
            @Override
            public Object[] apply(RocksKeyValue keyValue) {
                try {
                    return codec.decode(keyValue);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
    }

    public boolean contains(byte[] key) {
        tryOpenReadOnly();
        try {
            return dbReadOnly.keyMayExist(key, null) && dbReadOnly.get(key) != null;
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean insert(@Nonnull Object[] tuple) {
        tryOpenWritable();
        try {
            RocksKeyValue keyValue = codec.encode(tuple);
            if (!contains(keyValue.getKey())) {
                dbWritable.put(keyValue.getKey(), keyValue.getValue());
                refreshReading();
                return true;
            }
        } catch (IOException | RocksDBException e) {
            e.printStackTrace();
        }
        return false;
    }

    private void refreshReading() {
        // Force re-open for reading.
        if (dbReadOnly != null) {
            dbReadOnly.close();
            dbReadOnly = null;
        }
    }

    @Override
    public void upsert(@Nonnull Object[] tuple) {
        tryOpenWritable();
        try {
            RocksKeyValue keyValue = codec.encode(tuple);
            dbWritable.put(keyValue.getKey(), keyValue.getValue());
            refreshReading();
        } catch (IOException | RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean remove(@Nonnull Object[] tuple) {
        tryOpenWritable();
        try {
            RocksKeyValue keyValue = codec.encode(tuple);
            if (contains(keyValue.getKey())) {
                dbWritable.delete(keyValue.getKey());
                refreshReading();
                return true;
            }
        } catch (IOException | RocksDBException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    @Nullable
    public Object[] getByKey(@Nonnull Object[] keyTuple) {
        tryOpenReadOnly();
        try {
            byte[] key = codec.encodeKey(keyTuple);
            byte[] value = dbReadOnly.get(key);
            if (value != null) {
                return codec.mapKeyAndDecodeValue(keyTuple, value);
            }
        } catch (IOException | RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    @Nonnull
    public List<Object[]> getByMultiKey(@Nonnull final List<Object[]> keyTuples) {
        List<byte[]> keyList = keyTuples.stream()
            .map(k -> {
                try {
                    return codec.encodeKey(k);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        List<Object[]> tuples = new ArrayList<>(keyList.size());
        try {
            List<byte[]> valueList = dbReadOnly.multiGetAsList(keyList);
            for (int i = 0; i < keyList.size(); ++i) {
                tuples.add(codec.mapKeyAndDecodeValue(keyTuples.get(i), valueList.get(i)));
            }
        } catch (RocksDBException | IOException e) {
            e.printStackTrace();
        }
        return tuples;
    }
}
