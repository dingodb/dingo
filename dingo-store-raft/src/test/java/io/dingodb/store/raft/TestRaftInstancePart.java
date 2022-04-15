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

package io.dingodb.store.raft;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.util.Files;
import io.dingodb.raft.kv.storage.MemoryRawKVStore;
import io.dingodb.store.api.KeyValue;
import io.dingodb.store.api.Part;
import io.dingodb.store.raft.config.StoreConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRaftInstancePart {

    private static RaftStoreInstancePart storeInstancePart;
    private static MemoryRawKVStore store = new MemoryRawKVStore();
    private static Location location;

    @BeforeAll
    public static void beforeAll() throws Exception {
        DingoConfiguration.parse(TestRaftInstancePart.class.getResource("/TestStoreInstanceSegment.yaml").getPath());
        location = DingoConfiguration.location();
        Part part = Part.builder()
            .id(CommonId.prefix((byte) 'T'))
            .replicates(singletonList(location))
            .build();
        storeInstancePart = new RaftStoreInstancePart(part, store);
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
    }

    @AfterAll
    public static void afterAll() throws Exception {
        Files.deleteIfExists(Paths.get(StoreConfiguration.dbPath()));
        Files.deleteIfExists(Paths.get(StoreConfiguration.raft().getRaftPath()));
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
        store.iterator()
            .forEachRemaining(keyValue -> store.delete(keyValue.getKey()));
    }

    @Test
    public void testGetValueByPrimaryKey() {
        assertThat(storeInstancePart.getValueByPrimaryKey(new byte[] {2})).isEqualTo(new byte[] {2});
    }

    @Test
    public void testGetKeyValueByPrimaryKeys() {
        assertThat(storeInstancePart.getKeyValueByPrimaryKeys(Arrays.asList(new byte[] {1}, new byte[] {3})))
            .hasSize(2)
            .contains(
                new KeyValue(new byte[] {1}, new byte[] {1}),
                new KeyValue(new byte[] {3}, new byte[] {3})
            );
    }

    @Test
    public void testExist() {
        assertThat(storeInstancePart.exist(new byte[] {2})).isTrue();
    }

    @Test
    public void testKeyValueScan() {
        Iterator<KeyValue> iterator = storeInstancePart.keyValueScan(new byte[] {2}, new byte[] {4});
        assertThat(iterator.next()).isEqualTo(new KeyValue(new byte[] {2}, new byte[] {2}));
        assertThat(iterator.next()).isEqualTo(new KeyValue(new byte[] {3}, new byte[] {3}));
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testUpsertKeyValue() {
        storeInstancePart.upsertKeyValue(new byte[] {20}, new byte[] {20});
        assertThat(storeInstancePart.getValueByPrimaryKey(new byte[] {20})).isEqualTo(new byte[] {20});
    }

    @Test
    public void testUpsertKeyValueList() {
        storeInstancePart.upsertKeyValue(Arrays.asList(
            new KeyValue(new byte[] {11}, new byte[] {11}),
            new KeyValue(new byte[] {12}, new byte[] {12}),
            new KeyValue(new byte[] {13}, new byte[] {13}),
            new KeyValue(new byte[] {14}, new byte[] {14})
        ));
        assertThat(storeInstancePart.getValueByPrimaryKey(new byte[] {11})).isEqualTo(new byte[] {11});
        assertThat(storeInstancePart.getValueByPrimaryKey(new byte[] {12})).isEqualTo(new byte[] {12});
        assertThat(storeInstancePart.getValueByPrimaryKey(new byte[] {13})).isEqualTo(new byte[] {13});
        assertThat(storeInstancePart.getValueByPrimaryKey(new byte[] {14})).isEqualTo(new byte[] {14});
    }


    @Test
    public void testDelete() {
        storeInstancePart.delete(new byte[] {2});
        assertThat(storeInstancePart.exist(new byte[] {1})).isTrue();
        assertThat(storeInstancePart.exist(new byte[] {2})).isFalse();
        assertThat(storeInstancePart.exist(new byte[] {3})).isTrue();
        assertThat(storeInstancePart.exist(new byte[] {4})).isTrue();
        assertThat(storeInstancePart.exist(new byte[] {5})).isTrue();
    }

    @Test
    public void testDeleteList() {
        storeInstancePart.delete(Arrays.asList(new byte[] {2}, new byte[] {4}));
        assertThat(storeInstancePart.exist(new byte[] {1})).isTrue();
        assertThat(storeInstancePart.exist(new byte[] {2})).isFalse();
        assertThat(storeInstancePart.exist(new byte[] {3})).isTrue();
        assertThat(storeInstancePart.exist(new byte[] {4})).isFalse();
        assertThat(storeInstancePart.exist(new byte[] {5})).isTrue();
    }

    @Test
    public void testDeleteRange() {
        storeInstancePart.delete(new byte[] {2}, new byte[] {4});
        assertThat(storeInstancePart.exist(new byte[] {1})).isTrue();
        assertThat(storeInstancePart.exist(new byte[] {2})).isFalse();
        assertThat(storeInstancePart.exist(new byte[] {3})).isFalse();
        assertThat(storeInstancePart.exist(new byte[] {4})).isTrue();
        assertThat(storeInstancePart.exist(new byte[] {5})).isTrue();
    }

}
