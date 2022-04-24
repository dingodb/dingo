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

import io.dingodb.common.util.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.zip.Checksum;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMemoryRawKVStore {

    private MemoryRawKVStore store = new MemoryRawKVStore();

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
        store.defaultDB().clear();
    }

    @Test
    public void testScan() {
        SeekableIterator<byte[], ByteArrayEntry> iterator = store.scan(new byte[] {2}, new byte[] {4});
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {2}, new byte[] {2}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {3}, new byte[] {3}));
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
    public void testSnapshot() throws Exception {
        Path snapshot = Paths.get(getClass().getName());
        String path = snapshot.toString();
        Checksum checksum = store.snapshotSave(path).get();
        store.defaultDB().clear();
        assertThat(store.defaultDB()).hasSize(0);

        store.snapshotLoad(path, Long.toHexString(checksum.getValue())).get();
        SeekableIterator<byte[], ByteArrayEntry> iterator = store.iterator();
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {1}, new byte[] {1}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {2}, new byte[] {2}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {3}, new byte[] {3}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {4}, new byte[] {4}));
        assertThat(iterator.next()).isEqualTo(new ByteArrayEntry(new byte[] {5}, new byte[] {5}));
        assertThat(iterator.hasNext()).isFalse();

        Files.deleteIfExists(snapshot);
    }

}
