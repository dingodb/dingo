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

package io.dingodb.raft.kv.storage;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.util.Files;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.core.DefaultJRaftServiceFactory;
import io.dingodb.raft.option.NodeOptions;
import io.dingodb.raft.option.RaftLogStorageOptions;
import io.dingodb.raft.option.RaftLogStoreOptions;
import io.dingodb.raft.storage.LogStorage;
import io.dingodb.raft.storage.impl.RocksDBLogStorage;
import io.dingodb.raft.storage.impl.RocksDBLogStore;
import io.dingodb.raft.util.Endpoint;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRaftRawKVStore {

    public static final CommonId RAFT_ID = new CommonId(
        (byte) 'T', new byte[] {'T', 'R'}, PrimitiveCodec.encodeInt(0), PrimitiveCodec.encodeInt(0));
    public static final String SRV_LIST = "localhost:9181";
    public static final String DB_PATH = TestRaftRawKVStore.class.getName();


    private static RaftRawKVStore store;
    private static MemoryRawKVStore STORE = new MemoryRawKVStore();

    @BeforeAll
    public static void beforeAll() throws Exception {
        afterAll();
        Endpoint endpoint = new Endpoint("localhost", 9181);
        store = new RaftRawKVStore(
            RAFT_ID, STORE, createNodeOptions(), new Location(endpoint.getIp(), endpoint.getPort())
        );
        store.init(null);
        while (!store.getNode().isLeader()) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
    }

    @AfterAll
    public static void afterAll() throws Exception {
        Files.deleteIfExists(Paths.get(DB_PATH));
    }


    private static NodeOptions createNodeOptions() {
        final Configuration initialConf = new Configuration();
        initialConf.parse(SRV_LIST);
        NodeOptions nodeOpts = new NodeOptions();
        nodeOpts.setInitialConf(initialConf);
        final String logUri = Paths.get(DB_PATH, "log").toString();
        try {
            FileUtils.forceMkdir(new File(logUri));
        } catch (final Throwable t) {
            throw new RuntimeException("Fail to make dir for logDbPath: " + logUri);
        }
        final RocksDBLogStore logStore = new RocksDBLogStore();
        RaftLogStoreOptions logStoreOptions = new RaftLogStoreOptions();
        logStoreOptions.setDataPath(logUri);
        logStoreOptions.setRaftLogStorageOptions(new RaftLogStorageOptions());
        logStoreOptions.setLogEntryCodecFactory(DefaultJRaftServiceFactory.newInstance().createLogEntryCodecFactory());
        logStore.init(logStoreOptions);
        LogStorage logStorage = new RocksDBLogStorage("test".getBytes(StandardCharsets.UTF_8), logStore);
        nodeOpts.setLogStorage(logStorage);
        String meteUri = Paths.get(DB_PATH, "meta").toString();
        nodeOpts.setRaftMetaUri(meteUri);
        String snapshotUri = Paths.get(DB_PATH, "snapshot").toString();
        nodeOpts.setSnapshotUri(snapshotUri);
        return nodeOpts;
    }

    @BeforeEach
    public void beforeEach() {
        STORE.put(new byte[] {1}, new byte[] {1});
        STORE.put(new byte[] {2}, new byte[] {2});
        STORE.put(new byte[] {3}, new byte[] {3});
        STORE.put(new byte[] {4}, new byte[] {4});
        STORE.put(new byte[] {5}, new byte[] {5});
    }

    @AfterEach
    public void afterEach() {
        store.read(RaftRawKVOperation.sync());
        STORE.defaultDB().clear();
    }

    @Test
    public void testGet() throws ExecutionException, InterruptedException {
        assertThat(store.get(new byte[] {2}).get()).isEqualTo(new byte[] {2});
    }

    @Test
    public void testMultiGet() throws ExecutionException, InterruptedException {
        assertThat(store.get(Arrays.asList(new byte[] {1}, new byte[] {3})).get())
            .hasSize(2)
            .contains(
                new ByteArrayEntry(new byte[] {1}, new byte[] {1}),
                new ByteArrayEntry(new byte[] {3}, new byte[] {3})
            );
    }

    @Test
    public void testContains() throws ExecutionException, InterruptedException {
        assertThat(store.containsKey(new byte[] {2}).get()).isTrue();
    }

    @Test
    public void testScan() throws ExecutionException, InterruptedException {
        SeekableIterator<byte[], ByteArrayEntry> iterator = store.scan(new byte[] {2}, new byte[] {4}).get();
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {2}, new byte[] {2}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {3}, new byte[] {3}));
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testPut() throws ExecutionException, InterruptedException {
        store.put(new byte[] {20}, new byte[] {20}).get();
        assertThat(store.get(new byte[] {20}).get()).isEqualTo(new byte[] {20});
    }

    @Test
    public void testPutList() throws ExecutionException, InterruptedException {
        store.put(Arrays.asList(
            new ByteArrayEntry(new byte[] {11}, new byte[] {11}),
            new ByteArrayEntry(new byte[] {12}, new byte[] {12}),
            new ByteArrayEntry(new byte[] {13}, new byte[] {13}),
            new ByteArrayEntry(new byte[] {14}, new byte[] {14})
        )).get();
        assertThat(store.get(new byte[] {11}).get()).isEqualTo(new byte[] {11});
        assertThat(store.get(new byte[] {12}).get()).isEqualTo(new byte[] {12});
        assertThat(store.get(new byte[] {13}).get()).isEqualTo(new byte[] {13});
        assertThat(store.get(new byte[] {14}).get()).isEqualTo(new byte[] {14});
    }


    @Test
    public void testDelete() throws ExecutionException, InterruptedException {
        store.delete(new byte[] {2}).get();
        assertThat(store.containsKey(new byte[] {1}).get()).isTrue();
        assertThat(store.containsKey(new byte[] {2}).get()).isFalse();
        assertThat(store.containsKey(new byte[] {3}).get()).isTrue();
        assertThat(store.containsKey(new byte[] {4}).get()).isTrue();
        assertThat(store.containsKey(new byte[] {5}).get()).isTrue();
    }

    @Test
    public void testDeleteList() throws ExecutionException, InterruptedException {
        store.delete(Arrays.asList(new byte[] {2}, new byte[] {4})).get();
        assertThat(store.containsKey(new byte[] {1}).get()).isTrue();
        assertThat(store.containsKey(new byte[] {2}).get()).isFalse();
        assertThat(store.containsKey(new byte[] {3}).get()).isTrue();
        assertThat(store.containsKey(new byte[] {4}).get()).isFalse();
        assertThat(store.containsKey(new byte[] {5}).get()).isTrue();
    }

    @Test
    public void testDeleteRange() throws ExecutionException, InterruptedException {
        store.delete(new byte[] {2}, new byte[] {4}).get();
        assertThat(store.containsKey(new byte[] {1}).get()).isTrue();
        assertThat(store.containsKey(new byte[] {2}).get()).isFalse();
        assertThat(store.containsKey(new byte[] {3}).get()).isFalse();
        assertThat(store.containsKey(new byte[] {4}).get()).isTrue();
        assertThat(store.containsKey(new byte[] {5}).get()).isTrue();
    }

}
