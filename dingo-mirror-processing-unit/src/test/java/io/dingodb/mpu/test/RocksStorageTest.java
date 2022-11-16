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

package io.dingodb.mpu.test;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.storage.rocks.RocksStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import io.dingodb.common.util.FileUtils;

import org.rocksdb.*;

import java.io.File;
import java.nio.file.Files;

public class RocksStorageTest {
    static {
        RocksDB.loadLibrary();
    }

    public RocksStorage storage;

    public void createRocksStorage() {
        try {
            // TODO generate useful parameters
            byte type = 0;
            CommonId id = CommonId.prefix(type);
            CommonId coreId = CommonId.prefix(type);
            CommonId mpuId = CommonId.prefix(type);
            Location location = new Location("127.0.0.1", 8000);
            int priority = 0;
            CoreMeta coreMeta = new CoreMeta(id, coreId, mpuId, location, priority);
            String test_db_path = String.format("/tmp/testRocksStorage-%d", System.nanoTime());

            storage = new RocksStorage(coreMeta, test_db_path,
                "", "", 0);

            Assertions.assertNotNull(storage);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanupRocksStorage() {
        try {
            storage.destroy();
            FileUtils.deleteIfExists(storage.path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCreateRocksStorage() {
        Assertions.assertDoesNotThrow(() -> createRocksStorage());
        Assertions.assertNotNull(storage);
        Assertions.assertDoesNotThrow(() -> cleanupRocksStorage());
    }

    @Test
    public void testCheckpoint() {
        Assertions.assertDoesNotThrow(() -> createRocksStorage());
        Assertions.assertNotNull(storage);

        Assertions.assertDoesNotThrow(()->
            {
                // recreate db, don't use EventListener
                // close and clean old db
                storage.checkpoint.close();
                storage.db.close();
                storage.instruction.close();
                FileUtils.deleteIfExists(storage.dbPath);
                FileUtils.deleteIfExists(storage.instructionPath);

                //create new db
                Options options = new Options();
                options.setCreateIfMissing(true);
                options.setCompressionType(CompressionType.NO_COMPRESSION);

                storage.db = RocksDB.open(options, storage.dbPath.toString());
                storage.instruction = RocksDB.open(options, storage.instructionPath.toString());
                storage.checkpoint = Checkpoint.create(storage.db);

                //put test data into new db
                String test_key = "test_key";
                for (int i = 0; i < 1000; i++) {
                    storage.db.put(String.format("test_key_%d", i).getBytes(), test_key.getBytes());
                    storage.db.put(test_key.getBytes(), test_key.getBytes());
                }

                //create new checkpoint
                storage.createNewCheckpoint();
                storage.createNewCheckpoint();
                storage.createNewCheckpoint();

                File[] directories = new File(storage.checkpointPath.toString()).listFiles(File::isDirectory);
                Assertions.assertEquals(directories.length, 3);

                // test GetLatestCheckpointName
                String latest_checkpoint_name = storage.GetLatestCheckpointName(storage.LOCAL_CHECKPOINT_PREFIX);
                for (File checkpoint_dir : directories) {
                    Assertions.assertTrue(checkpoint_dir.getName().compareTo(latest_checkpoint_name) <= 0);
                }

                // test purgeOldCheckpoint
                storage.purgeOldCheckpoint(2);
                directories = new File(storage.checkpointPath.toString()).listFiles(File::isDirectory);
                Assertions.assertEquals(directories.length, 2);

                // test restoreFromCheckpoint
                storage.db.delete(test_key.getBytes());
                byte[] value = storage.db.get(test_key.getBytes());
                Assertions.assertNull(value);

                // make a fake remote checkpoint
                Files.move(
                    storage.checkpointPath.resolve(latest_checkpoint_name),
                    storage.path.resolve(
                        String.format("%s%s", storage.REMOTE_CHECKPOINT_PREFIX, "checkpoint"
                        )
                    )
                );
                storage.applyBackup();
                value = storage.db.get(test_key.getBytes());
                Assertions.assertEquals(new String(value), test_key);
            }
        );
        Assertions.assertDoesNotThrow(() -> cleanupRocksStorage());
    }
}
