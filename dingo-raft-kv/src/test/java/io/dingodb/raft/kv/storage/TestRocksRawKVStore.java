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

import io.dingodb.common.util.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.zip.Checksum;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRocksRawKVStore {

    private static RocksRawKVStore store;
    public static final Path DB_PATH = Paths.get(TestRaftRawKVStore.class.getName());

    @BeforeAll
    public static void beforeAll() throws Exception {
        afterAll();
        store = new RocksRawKVStore(DB_PATH.toString(), "", "");
    }

    @AfterAll
    public static void afterAll() throws Exception {
        if (store != null) {
            store.close();
        }
        FileUtils.deleteIfExists(DB_PATH);
    }

    @BeforeEach
    public void beforeEach() {
        store.put(new byte[] {1}, new byte[] {1});
        store.put(new byte[] {2}, new byte[] {2});
        store.put(new byte[] {3}, new byte[] {3});
        store.put(new byte[] {4}, new byte[] {4});
        store.put(new byte[] {5}, new byte[] {5});
    }

    @AfterEach
    public void afterEach() {
        store.delete(new byte[] {1}, new byte[] {Byte.MAX_VALUE});
    }

    @Test
    public void testScan() {
        SeekableIterator<byte[], ByteArrayEntry> iterator = store.scan(new byte[] {2}, new byte[] {4});
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {2}, new byte[] {2}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {3}, new byte[] {3}));
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testRangeScan1() {
        SeekableIterator<byte[], ByteArrayEntry> iterator = store.scan(new byte[] {2}, new byte[] {4}, false, true);
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {3}, new byte[] {3}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {4}, new byte[] {4}));
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testRangeScan2() {
        SeekableIterator<byte[], ByteArrayEntry> iterator = store.scan(new byte[] {2}, new byte[] {4}, true, true);
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {2}, new byte[] {2}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {3}, new byte[] {3}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {4}, new byte[] {4}));
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testDeleteRange() {
        store.delete(new byte[] {2}, new byte[] {4});
        assertThat(store.containsKey(new byte[] {1})).isTrue();
        assertThat(store.containsKey(new byte[] {2})).isFalse();
        assertThat(store.containsKey(new byte[] {3})).isFalse();
        assertThat(store.containsKey(new byte[] {4})).isTrue();
        assertThat(store.containsKey(new byte[] {5})).isTrue();
    }

    @Test
    public void testMultiGet() {
        assertThat(store.get(Arrays.asList(new byte[] {1}, new byte[] {3})))
            .hasSize(2)
            .contains(
                new ByteArrayEntry(new byte[] {1}, new byte[] {1}),
                new ByteArrayEntry(new byte[] {3}, new byte[] {3})
            );
    }

    @Test
    public void testPutList() {
        store.put(Arrays.asList(
            new ByteArrayEntry(new byte[] {11}, new byte[] {11}),
            new ByteArrayEntry(new byte[] {12}, new byte[] {12}),
            new ByteArrayEntry(new byte[] {13}, new byte[] {13}),
            new ByteArrayEntry(new byte[] {14}, new byte[] {14})
        ));
        assertThat(store.get(new byte[] {11})).isEqualTo(new byte[] {11});
        assertThat(store.get(new byte[] {12})).isEqualTo(new byte[] {12});
        assertThat(store.get(new byte[] {13})).isEqualTo(new byte[] {13});
        assertThat(store.get(new byte[] {14})).isEqualTo(new byte[] {14});
    }

    @Test
    public void testSnapshot() throws Exception {
        String path = DB_PATH.toString();
        Checksum checksum = store.snapshotSave(path).get();
        afterEach();
        assertThat(store.iterator().hasNext()).isFalse();

        store.snapshotLoad(path, Long.toHexString(checksum.getValue())).get();
        SeekableIterator<byte[], ByteArrayEntry> iterator = store.iterator();
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {1}, new byte[] {1}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {2}, new byte[] {2}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {3}, new byte[] {3}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {4}, new byte[] {4}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {5}, new byte[] {5}));
        assertThat(iterator.hasNext()).isFalse();
    }

}
