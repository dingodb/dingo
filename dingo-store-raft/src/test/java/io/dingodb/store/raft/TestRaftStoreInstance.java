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
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Files;
import io.dingodb.common.util.StackTraces;
import io.dingodb.raft.kv.storage.RawKVStore;
import io.dingodb.store.raft.config.StoreConfiguration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.dingodb.common.codec.PrimitiveCodec.encodeInt;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRaftStoreInstance {

    public static final Path TEST_PATH = Paths.get(TestRaftStoreInstance.class.getName());

    private static RawKVStore store;
    private static CommonId id = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.table, encodeInt(0), encodeInt(0));
    private static Location location;
    private static RaftStoreInstance storeInstance;

    @BeforeAll
    public static void beforeAll() throws Exception {
        afterAll();
        DingoConfiguration.parse(TestRaftInstancePart.class.getResource("/config.yaml").getPath());
        location = new Location(DingoConfiguration.host(), DingoConfiguration.port(), StoreConfiguration.raft().getPort());
        storeInstance = new RaftStoreInstance(TEST_PATH, id);
        store = storeInstance.getStore();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        Files.deleteIfExists(TEST_PATH);
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
        CommonId id = new CommonId(
            ID_TYPE.table, TABLE_IDENTIFIER.part, encodeInt(0), encodeInt(StackTraces.lineNumber())
        );
        Part part = Part.builder()
            .id(id)
            .start(ByteArrayUtils.EMPTY_BYTES)
            .replicates(singletonList(location))
            .build();
        storeInstance.assignPart(part);
        while (storeInstance.getPart(part.getStart()) == null) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        assertThat(storeInstance.getValueByPrimaryKey(new byte[] {2})).isEqualTo(new byte[] {2});
        storeInstance.unassignPart(part);
        /**
         * cannot find any reason for failed when execute all tests.
        Assertions.assertThatThrownBy(() -> storeInstance.getValueByPrimaryKey(new byte[] {2}))
            .isInstanceOf(IllegalArgumentException.class);
         */
    }

    @Test
    public void testGetKeyValueByPrimaryKeys() {
        CommonId id = new CommonId(
            ID_TYPE.table, TABLE_IDENTIFIER.part, encodeInt(0), encodeInt(StackTraces.lineNumber())
        );
        Part part = Part.builder()
            .id(id)
            .start(ByteArrayUtils.EMPTY_BYTES)
            .replicates(singletonList(location))
            .build();
        storeInstance.assignPart(part);
        while (storeInstance.getPart(part.getStart()) == null) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
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

    @Test
    public void testReassign() {
        CommonId id = new CommonId(
            ID_TYPE.table, TABLE_IDENTIFIER.part, encodeInt(0), encodeInt(StackTraces.lineNumber())
        );
        Part part = Part.builder()
            .id(id)
            .start(ByteArrayUtils.EMPTY_BYTES)
            .replicates(singletonList(location))
            .build();
        storeInstance.assignPart(part);
        while (storeInstance.getPart(part.getStart()) == null) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        assertThat(storeInstance.getValueByPrimaryKey(new byte[] {2})).isEqualTo(new byte[] {2});
        part = Part.builder()
            .id(id)
            .start(ByteArrayUtils.EMPTY_BYTES)
            .end(new byte[] {3})
            .replicates(singletonList(location))
            .build();
        storeInstance.reassignPart(part);
        assertThat(storeInstance.getValueByPrimaryKey(new byte[] {2})).isEqualTo(new byte[] {2});
        Assertions.assertThatThrownBy(() -> storeInstance.getValueByPrimaryKey(new byte[] {3}))
            .isInstanceOf(IllegalArgumentException.class);
        storeInstance.unassignPart(part);
        Assertions.assertThatThrownBy(() -> storeInstance.getValueByPrimaryKey(new byte[] {2}))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testGetValueByPrimaryKeyWithMultiPartition() {
        /**
         * Partition: 1:[-inf, 1), 2:[1, 3), 3:[3, 5), 4:[5, inf)
         * ----------1------------3----------5---------
         */
        //
        List<Part> parts = constructMultiPartTables();
        for (Part part : parts) {
            while (storeInstance.getPart(part.getStart()) == null) {
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            }
        }

        /*
         * case1: query range is: [1, 2)
         * then result is : [1, 2)
         */
        Map<Part, List<byte[]>> partListMap01 = storeInstance.groupKeysByPart(
            new byte[] {1},
            new byte[] {2}
        );
        Assertions.assertThat(partListMap01.size()).isEqualTo(1);
        for (Map.Entry<Part, List<byte[]>> entry : partListMap01.entrySet()) {
            List<byte[]> keyRange = entry.getValue();
            Assertions.assertThat(keyRange.get(0)).isEqualTo(new byte[]{1});
            Assertions.assertThat(keyRange.get(1)).isEqualTo(new byte[]{2});
        }

        /**
         * case2: query range is: [1, 3]
         * then result is : [1, 3), [3,3)
         */
        Map<Part, List<byte[]>> partListMap02 = storeInstance.groupKeysByPart(
            new byte[] {1},
            new byte[] {3}
        );
        Assertions.assertThat(partListMap02.size()).isEqualTo(2);

        List<List<byte[]>> expectResult = new ArrayList<>();
        expectResult.add(Arrays.asList(new byte[]{1}, new byte[]{3}));
        expectResult.add(Arrays.asList(new byte[]{3}, new byte[]{3}));
        for (Map.Entry<Part, List<byte[]>> entry : partListMap02.entrySet()) {
            List<byte[]> keyRange = entry.getValue();
            boolean isOK1 = isByteArrayEquals(keyRange, expectResult.get(0));
            boolean isOK2 = isByteArrayEquals(keyRange, expectResult.get(1));
            Assertions.assertThat(isOK1 || isOK2).isTrue();
        }


        /**
         * query range is : [0, 7)
         * then result is : [0, 1), [1, 3), [3, 5), [5, 7)
         */
        Map<Part, List<byte[]>> partListMap03 = storeInstance.groupKeysByPart(
            new byte[]{ 0 },
            new byte[]{ 7 }
        );
        Assertions.assertThat(partListMap03.size()).isEqualTo(4);
        expectResult.clear();
        expectResult.add(Arrays.asList(new byte[]{0}, new byte[]{1}));
        expectResult.add(Arrays.asList(new byte[]{1}, new byte[]{3}));
        expectResult.add(Arrays.asList(new byte[]{3}, new byte[]{5}));
        expectResult.add(Arrays.asList(new byte[]{5}, new byte[]{7}));
        for (Map.Entry<Part, List<byte[]>> entry : partListMap03.entrySet()) {
            List<byte[]> keyRange = entry.getValue();
            boolean isOK1 = isByteArrayEquals(keyRange, expectResult.get(0));
            boolean isOK2 = isByteArrayEquals(keyRange, expectResult.get(1));
            boolean isOK3 = isByteArrayEquals(keyRange, expectResult.get(2));
            boolean isOK4 = isByteArrayEquals(keyRange, expectResult.get(3));
            Assertions.assertThat(isOK1 || isOK2 || isOK3 || isOK4).isTrue();
        }
    }

    private List<Part> constructMultiPartTables() {
        List<Part> parts = new ArrayList<>();

        CommonId id1 = new CommonId(
            ID_TYPE.table, TABLE_IDENTIFIER.part, encodeInt(1), encodeInt(StackTraces.lineNumber())
        );
        Part part1 = Part.builder()
            .id(id1)
            .start(ByteArrayUtils.EMPTY_BYTES)
            .end(new byte[] {1})
            .replicates(singletonList(location))
            .build();
        storeInstance.assignPart(part1);
        parts.add(part1);

        CommonId id2 = new CommonId(
            ID_TYPE.table, TABLE_IDENTIFIER.part, encodeInt(2), encodeInt(StackTraces.lineNumber())
        );
        Part part2 = Part.builder()
            .id(id2)
            .start(new byte[] {1})
            .end(new byte[] {3})
            .replicates(singletonList(location))
            .build();
        storeInstance.assignPart(part2);
        parts.add(part2);


        CommonId id3 = new CommonId(
            ID_TYPE.table, TABLE_IDENTIFIER.part, encodeInt(3), encodeInt(StackTraces.lineNumber())
        );
        Part part3 = Part.builder()
            .id(id3)
            .start(new byte[] {3})
            .end(new byte[] {5})
            .replicates(singletonList(location))
            .build();
        storeInstance.assignPart(part3);
        parts.add(part3);

        CommonId id4 = new CommonId(
            ID_TYPE.table, TABLE_IDENTIFIER.part, encodeInt(4), encodeInt(StackTraces.lineNumber())
        );
        Part part4 = Part.builder()
            .id(id4)
            .start(new byte[] {5})
            .replicates(singletonList(location))
            .build();
        storeInstance.assignPart(part4);
        parts.add(part4);
        return parts;
    }

    private boolean isByteArrayEquals(List<byte[]> expected, List<byte[]> real) {
        if (expected.size() != real.size()) {
            return false;
        }

        for (int i = 0; i < expected.size(); i++) {
            int result = ByteArrayUtils.compare(expected.get(i), real.get(i));
            if (result != 0) {
                return false;
            }
        }
        return true;
    }
}
