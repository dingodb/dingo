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
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Files;
import io.dingodb.raft.kv.storage.RawKVStore;
import io.dingodb.store.api.KeyValue;
import io.dingodb.store.api.Part;
import io.dingodb.store.raft.config.StoreConfiguration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.dingodb.common.codec.PrimitiveCodec.encodeInt;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRaftStoreInstance {

    private static RawKVStore store;
    private static CommonId id = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.table, encodeInt(0), encodeInt(0));
    private static Location location;
    private static RaftStoreInstance storeInstance;

    @BeforeAll
    public static void beforeAll() throws Exception {
        DingoConfiguration.parse(TestRaftInstancePart.class.getResource("/TestRaftStoreInstance.yaml").getPath());
        location = new Location(DingoConfiguration.host(), StoreConfiguration.raft().getPort());
        storeInstance = new RaftStoreInstance(id);
        store = storeInstance.getStore();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        Files.deleteIfExists(Paths.get("dingo"));
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
        Files.deleteIfExists(Paths.get(StoreConfiguration.raft().getRaftPath()));
    }

    @Test
    public void testGetValueByPrimaryKey() {
        CommonId id = new CommonId(ID_TYPE.service, TABLE_IDENTIFIER.part, encodeInt(0), encodeInt(0));
        Part part = Part.builder()
            .id(id)
            .start(ByteArrayUtils.MIN_BYTES)
            .end(ByteArrayUtils.MAX_BYTES)
            .replicates(singletonList(location))
            .build();
        storeInstance.assignPart(part);
        while (!storeInstance.getPart(id).getStateMachine().isAvailable()) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(3));
        }
        assertThat(storeInstance.getValueByPrimaryKey(new byte[] {2})).isEqualTo(new byte[] {2});
        storeInstance.unassignPart(part);
        Assertions.assertThatThrownBy(() -> storeInstance.getValueByPrimaryKey(new byte[] {2}))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testGetKeyValueByPrimaryKeys() {
        CommonId id = new CommonId(ID_TYPE.service, TABLE_IDENTIFIER.part, encodeInt(0), encodeInt(0));
        Part part = Part.builder()
            .id(id)
            .start(ByteArrayUtils.MIN_BYTES)
            .end(ByteArrayUtils.MAX_BYTES)
            .replicates(singletonList(location))
            .build();
        storeInstance.assignPart(part);
        while (!storeInstance.getPart(id).getStateMachine().isAvailable()) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(3));
        }
        assertThat(storeInstance.getKeyValueByPrimaryKeys(Arrays.asList(new byte[] {1}, new byte[] {3})))
            .hasSize(2)
            .contains(
                new KeyValue(new byte[] {1}, new byte[] {1}),
                new KeyValue(new byte[] {3}, new byte[] {3})
            );
        storeInstance.unassignPart(part);
        Assertions.assertThatThrownBy(
            () -> storeInstance.getKeyValueByPrimaryKeys(Arrays.asList(new byte[] {1}, new byte[] {3}))
        ).isInstanceOf(IllegalArgumentException.class);
    }

}
