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

package io.dingodb.partition.base;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConsistentHashingTest {
    private ConsistentHashing<Long> consistentHashing;

    @BeforeAll
    public void setUp() {
        consistentHashing = new ConsistentHashing<>(3);
    }

    @Test
    public void testAddNode() {
        consistentHashing.addNode(1l);
        consistentHashing.addNode(2l);
        consistentHashing.addNode(3l);
        byte[] key = "aaa".getBytes(StandardCharsets.UTF_8);
        Long node = consistentHashing.getNode(key);
        System.out.println(node);
    }

    @Test
    public void testRemoveNode() {
        consistentHashing.addNode(1l);
        consistentHashing.addNode(2l);
        consistentHashing.addNode(3l);
        byte[] key = "aaa".getBytes(StandardCharsets.UTF_8);
        Long node = consistentHashing.getNode(key);
        System.out.println(node);
        consistentHashing.removeNode(1l);
        Long node1 = consistentHashing.getNode(key);
        System.out.println(node1);
        Assertions.assertEquals(node, node1);
        consistentHashing.removeNode(2l);
        Long node2 = consistentHashing.getNode(key);
        System.out.println(node2);
    }

    @Test
    public void testGetNode() {
        consistentHashing.addNode(11111111l);
        consistentHashing.addNode(22222222l);
        consistentHashing.addNode(33333333l);
        byte[] key = "test".getBytes(StandardCharsets.UTF_8);
        Long node = consistentHashing.getNode(key);
        Assertions.assertNotNull(node);
    }

    @Test
    public void testGetNodeWithEmptyRing() {
        byte[] key = "test".getBytes(StandardCharsets.UTF_8);
        Long node = consistentHashing.getNode(key);
        Assertions.assertNotNull(node);
    }

    @Test
    public void testGetNodeWithSameKey() {
        consistentHashing.addNode(11111111l);
        consistentHashing.addNode(22222222l);
        consistentHashing.addNode(33333333l);
        byte[] key1 = "test".getBytes(StandardCharsets.UTF_8);
        byte[] key2 = "test".getBytes(StandardCharsets.UTF_8);
        Long node1 = consistentHashing.getNode(key1);
        Long node2 = consistentHashing.getNode(key2);
        Assertions.assertEquals(node1, node2);
        byte[] key3 = "中文".getBytes(StandardCharsets.UTF_8);
        byte[] key4 = "中文".getBytes(StandardCharsets.UTF_8);
        Long node3 = consistentHashing.getNode(key3);
        Long node4 = consistentHashing.getNode(key4);
        Assertions.assertEquals(node3, node4);

    }
}
