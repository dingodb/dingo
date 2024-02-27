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

package io.dingodb.store.local;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.FileUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreServiceProvider;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class StoreService implements io.dingodb.store.api.StoreService {

    protected static final RocksDB db;

    static {
        String path = Configuration.path();
        if (path == null) {
            path = "/tmp/dingodb/store";
        }
        RocksDB rocksdb = null;
        try {
            Path dbPath = Paths.get(path);
            FileUtils.deleteIfExists(dbPath);
            FileUtils.createDirectories(dbPath);
            Options options = new Options();
            options.setNumLevels(1);
            options.setCreateIfMissing(true);
            options.setWriteBufferSize(Configuration.instance().getBufferSize());
            options.setMaxWriteBufferNumber(Configuration.instance().getBufferNumber());
            options.setTargetFileSizeBase(Configuration.instance().getFileSize());
            rocksdb = RocksDB.open(path);
        } catch (Exception e) {
            log.info("No local db.", e);
        }
        db = rocksdb;
    }

    public static final StoreService INSTANCE = new StoreService();

    @AutoService(StoreServiceProvider.class)
    public static class Provider implements StoreServiceProvider {

        @Override
        public String key() {
            return "local";
        }

        @Override
        public io.dingodb.store.api.StoreService get() {
            return INSTANCE;
        }
    }

    @Override
    public StoreInstance getInstance(@NonNull CommonId tableId, CommonId regionId) {
        return new io.dingodb.store.local.StoreInstance(regionId);
    }

//    @Override
//    public StoreInstance getInstance(@NonNull CommonId tableId, CommonId regionId, TableDefinition tableDefinition) {
//        return new io.dingodb.store.local.StoreInstance(regionId);
//    }
}
