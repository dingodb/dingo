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
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.util.FileUtils;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.storage.rocks.ColumnFamilyConfiguration;
import io.dingodb.mpu.storage.rocks.RocksConfiguration;
import io.dingodb.mpu.storage.rocks.RocksStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.rocksdb.Checkpoint;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.FileWriter;
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
            byte[] identifier = new byte[] {1, 1};
            CommonId id = CommonId.prefix(type, identifier);
            CommonId coreId = CommonId.prefix(type, identifier);
            CommonId mpuId = CommonId.prefix(type, identifier);
            Location location = new Location("127.0.0.1", 8000);
            int priority = 0;
            CoreMeta coreMeta = new CoreMeta(id, coreId, mpuId, location, priority);
            String testDbPath = String.format("/tmp/testRocksStorage-%d", System.nanoTime());

            //String tmpDingoConfigPath = genTmpConfigFile();
            //DingoConfiguration.parse(tmpDingoConfigPath);

            storage = new RocksStorage(coreMeta, testDbPath,
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

    /*@Test
    public void testCreateRocksStorage() {
        Assertions.assertDoesNotThrow(() -> createRocksStorage());
        Assertions.assertNotNull(storage);
        Assertions.assertDoesNotThrow(() -> cleanupRocksStorage());
    }*/

    @Test
    public void testCheckpoint() {
        Assertions.assertDoesNotThrow(() -> createRocksStorage());
        Assertions.assertNotNull(storage);

        Assertions.assertDoesNotThrow(() -> {
                // recreate db, don't use EventListener
                // close and clean old db
                storage.checkPoint.close();
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
                storage.checkPoint = Checkpoint.create(storage.db);

                //put test data into new db
                String testKey = "test_key";
                for (int i = 0; i < 1000; i++) {
                    storage.db.put(String.format("test_key_%d", i).getBytes(), testKey.getBytes());
                    storage.db.put(testKey.getBytes(), testKey.getBytes());
                }

                //create new checkpoint
                storage.createNewCheckpoint();
                storage.createNewCheckpoint();
                storage.createNewCheckpoint();

                File[] directories = new File(storage.checkpointPath.toString()).listFiles(File::isDirectory);
                Assertions.assertEquals(directories.length, 3);

                // test GetLatestCheckpointName
                String latestCheckpointName = storage.getLatestCheckpointName(storage.LOCAL_CHECKPOINT_PREFIX);
                for (File checkpointDir : directories) {
                    Assertions.assertTrue(checkpointDir.getName().compareTo(latestCheckpointName) <= 0);
                }

                // test purgeOldCheckpoint
                storage.purgeOldCheckpoint(2);
                directories = new File(storage.checkpointPath.toString()).listFiles(File::isDirectory);
                Assertions.assertEquals(directories.length, 2);

                // test restoreFromCheckpoint
                storage.db.delete(testKey.getBytes());
                byte[] value = storage.db.get(testKey.getBytes());
                Assertions.assertNull(value);

                // make a fake remote checkpoint
                Files.move(
                    storage.checkpointPath.resolve(latestCheckpointName),
                    storage.path.resolve(
                        String.format("%s%s", storage.REMOTE_CHECKPOINT_PREFIX, "checkpoint"
                        )
                    )
                );
                storage.applyBackup();
                value = storage.db.get(testKey.getBytes());
                Assertions.assertEquals(new String(value), testKey);
            }
        );
        Assertions.assertDoesNotThrow(() -> cleanupRocksStorage());
    }

    public String genTmpConfigFile() {
        String tmpDingoConfigPath = "/tmp/dingo.yaml";
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("cluster:\n");
            sb.append("    name: dingo\n");
            sb.append("exchange:\n");
            sb.append("    host: server1\n");
            sb.append("    port: 19191\n");
            sb.append("server:\n");
            sb.append("    coordinatorExchangeSvrList: server1:19181,server1:19182,server1:19183\n");
            sb.append("    dataPath: /data/dingo_data/executor1/meta\n");
            sb.append("    monitorPort: 9091\n");
            sb.append("store:\n");
            sb.append("    dbPath: /data/dingo_data/executor1/raftDb\n");
            sb.append("    dbRocksOptionsFile: /home/user/dingo_bin/conf/db_rocks.ini\n");
            sb.append("    logRocksOptionsFile: /home/user/dingo_bin/conf/log_rocks.ini\n");
            sb.append("    dcfConfiguration:\n");
            sb.append("       cfMaxWriteBufferNumber: 5\n");
            sb.append("    raft:\n");
            sb.append("       port: 9191\n");
            sb.append("       raftPath: /data/dingo_data/executor1/raftLog\n");
            sb.append("    collectStatsInterval: 5\n");

            FileWriter tmpWriter = new FileWriter(tmpDingoConfigPath);
            tmpWriter.write(sb.toString());
            tmpWriter.close();
        } catch (Exception e) {
            System.out.println("genTmpConfigFile " + e.toString());
        }

        return tmpDingoConfigPath;
    }

    @Test
    public void testColumnFamilyConfiguration() {
        Assertions.assertDoesNotThrow(() -> {
            String tmpDingoConfigPath = genTmpConfigFile();
            DingoConfiguration.parse(tmpDingoConfigPath);

            RocksConfiguration rocksConfiguration = RocksConfiguration.refreshRocksConfiguration();
            ColumnFamilyConfiguration dcfConfig = rocksConfiguration.dcfConfiguration();
            Assertions.assertEquals(5, Integer.parseInt(dcfConfig.getCfMaxWriteBufferNumber()));
            Assertions.assertEquals(true, dcfConfig.getCfMaxCompactionBytes() == null);
        });
    }

    @Test
    public void testNoColumnFamilyConfiguration() {
        Assertions.assertDoesNotThrow(() -> {
            ColumnFamilyConfiguration dcfConfig = new ColumnFamilyConfiguration();
            Assertions.assertEquals(true, dcfConfig.getCfMaxCompactionBytes() == null);
        });
    }
}

