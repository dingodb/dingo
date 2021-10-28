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

package io.dingodb.kvstore;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Slf4j
public final class RocksBlock implements KvBlock {
    private final String path;

    private RocksDB dbReadOnly;
    private RocksDB dbWritable;

    public RocksBlock(String path) {
        this.path = path;
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
    public Iterator<KeyValue> getIterator() {
        tryOpenReadOnly();
        return new RocksBlockIterator(dbReadOnly);
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
    public void put(@Nonnull KeyValue keyValue) {
        tryOpenWritable();
        try {
            dbWritable.put(keyValue.getKey(), keyValue.getValue());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        refreshReading();
    }

    @Override
    public void delete(byte[] key) {
        try {
            dbWritable.delete(key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        refreshReading();
    }

    @Nullable
    @Override
    public byte[] get(byte[] key) {
        tryOpenReadOnly();
        try {
            return dbReadOnly.get(key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void refreshReading() {
        // Force re-open for reading.
        if (dbReadOnly != null) {
            dbReadOnly.close();
            dbReadOnly = null;
        }
    }

    @Override
    @Nonnull
    public List<byte[]> multiGet(@Nonnull final List<byte[]> keys) {
        try {
            return dbReadOnly.multiGetAsList(keys);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return ImmutableList.of();
    }
}
