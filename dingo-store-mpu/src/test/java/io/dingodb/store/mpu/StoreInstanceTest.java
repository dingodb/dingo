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

package io.dingodb.store.mpu;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.FileUtils;
import org.junit.jupiter.api.*;
import org.luaj.vm2.ast.Str;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

class StoreInstanceTest {

    private static final Path PATH = Paths.get(StoreInstance.class.getName());
    private static StoreInstance storeInstance;

    private List<String> readIterator(Iterator<KeyValue> it) {
        List<String> result = new ArrayList<String>();
        while (it.hasNext()) {
            KeyValue kv = it.next();
            result.add(new String(kv.getValue()));
            System.out.printf("%s %s%n", new String(kv.getKey()), new String(kv.getValue()));
        }

        return result;
    }

    @BeforeAll
    public static void beforeAll() {
        FileUtils.createDirectories(PATH);
        CommonId id = CommonId.prefix((byte) 'T');
        storeInstance = new StoreInstance(id, PATH);
        Part part = Part.builder()
            .id(id)
            .start(ByteArrayUtils.EMPTY_BYTES)
            .replicateLocations(Collections.singletonList(new Location("localhost", 0)))
            .replicateId(id)
            .replicates(Collections.singletonList(id))
            .build();
        storeInstance.assignPart(part);
    }

    @AfterAll
    public static void afterAll() {
        storeInstance.destroy();
        FileUtils.deleteIfExists(PATH);
    }

    @Test
    void testSetGet() {
        storeInstance.upsertKeyValue("test".getBytes(), "value".getBytes());

        Assertions.assertEquals("value", new String(storeInstance.getValueByPrimaryKey("test".getBytes())));

        storeInstance.delete("0".getBytes(), "zzzzzzzzzzzzz".getBytes());
    }

    @Test
    void testScanNotIncludeStart() {
        storeInstance.upsertKeyValue("1".getBytes(), "value01".getBytes());
        storeInstance.upsertKeyValue("2".getBytes(), "value02".getBytes());
        storeInstance.upsertKeyValue("3".getBytes(), "value03".getBytes());

        Iterator<KeyValue> it = storeInstance.keyValueScan("1".getBytes(), "2".getBytes(), false, true);

        List<String> result = readIterator(it);
        String[] actual = result.toArray(new String[result.size()]);

        String[] expected = {"value02"};
        Assertions.assertArrayEquals(expected, actual);

        storeInstance.delete("0".getBytes(), "zzzzzzzzzzzzz".getBytes());
    }

    @Test
    void testScanIncludeStart() {
        storeInstance.upsertKeyValue("1".getBytes(), "value01".getBytes());
        storeInstance.upsertKeyValue("2".getBytes(), "value02".getBytes());
        storeInstance.upsertKeyValue("3".getBytes(), "value03".getBytes());

        Iterator<KeyValue> it = storeInstance.keyValueScan("1".getBytes(), "2".getBytes(), true, true);

        List<String> result = readIterator(it);
        String[] actual = result.toArray(new String[result.size()]);

        String[] expected = {"value01", "value02"};
        Assertions.assertArrayEquals(expected, actual);

        storeInstance.delete("0".getBytes(), "zzzzzzzzzzzzz".getBytes());
    }

    @Test
    void testScanContinuousIncludeEnd() {
        storeInstance.upsertKeyValue("1".getBytes(), "value01".getBytes());
        storeInstance.upsertKeyValue("2".getBytes(), "value02".getBytes());
        storeInstance.upsertKeyValue("3".getBytes(), "value03".getBytes());

        Iterator<KeyValue> it = storeInstance.keyValueScan("1".getBytes(), "2".getBytes(), true, true);

        List<String> result = readIterator(it);
        String[] actual = result.toArray(new String[result.size()]);

        String[] expected = {"value01", "value02"};
        Assertions.assertArrayEquals(expected, actual);

        storeInstance.delete("0".getBytes(), "zzzzzzzzzzzzz".getBytes());
    }

    @Test
    void testScanContinuousNotIncludeEnd() {
        storeInstance.upsertKeyValue("1".getBytes(), "value01".getBytes());
        storeInstance.upsertKeyValue("2".getBytes(), "value02".getBytes());
        storeInstance.upsertKeyValue("3".getBytes(), "value03".getBytes());

        Iterator<KeyValue> it = storeInstance.keyValueScan("1".getBytes(), "2".getBytes(), true, false);

        List<String> result = readIterator(it);
        String[] actual = result.toArray(new String[result.size()]);

        String[] expected = {"value01"};
        Assertions.assertArrayEquals(expected, actual);

        storeInstance.delete("0".getBytes(), "zzzzzzzzzzzzz".getBytes());
    }

    @Test
    void testScanNotContinuousIncludeEnd() {
        storeInstance.upsertKeyValue("1".getBytes(), "value01".getBytes());
        storeInstance.upsertKeyValue("2".getBytes(), "value02".getBytes());
        storeInstance.upsertKeyValue("3".getBytes(), "value03".getBytes());

        Iterator<KeyValue> it = storeInstance.keyValueScan("1".getBytes(), "3".getBytes(), true, true);

        List<String> result = readIterator(it);
        String[] actual = result.toArray(new String[result.size()]);

        String[] expected = {"value01", "value02", "value03"};
        Assertions.assertArrayEquals(expected, actual);

        storeInstance.delete("0".getBytes(), "zzzzzzzzzzzzz".getBytes());
    }

    @Test
    void testScanNotContinuousNotIncludeEnd() {
        storeInstance.upsertKeyValue("1".getBytes(), "value01".getBytes());
        storeInstance.upsertKeyValue("2".getBytes(), "value02".getBytes());
        storeInstance.upsertKeyValue("3".getBytes(), "value03".getBytes());

        Iterator<KeyValue> it = storeInstance.keyValueScan("1".getBytes(), "3".getBytes(), true, false);

        List<String> result = readIterator(it);
        String[] actual = result.toArray(new String[result.size()]);

        String[] expected = {"value01", "value02"};
        Assertions.assertArrayEquals(expected, actual);

        storeInstance.delete("0".getBytes(), "zzzzzzzzzzzzz".getBytes());
    }

    @Test
    void testPrefixScan01() {
        storeInstance.upsertKeyValue("bbbb1000".getBytes(), "value01".getBytes());
        storeInstance.upsertKeyValue("cbbb10001".getBytes(), "value02".getBytes());
        storeInstance.upsertKeyValue("bbbb20002".getBytes(), "value03".getBytes());
        storeInstance.upsertKeyValue("cccb100043".getBytes(), "value04".getBytes());

        Iterator<KeyValue> it = storeInstance.keyValuePrefixScan("bbbb".getBytes());
        List<String> result = readIterator(it);
        System.out.printf("result size: %d%n", result.size());
        String[] actual = result.toArray(new String[result.size()]);

        String[] expected = {"value01", "value03"};
        Assertions.assertArrayEquals(expected, actual);

        storeInstance.delete("0".getBytes(), "zzzzzzzzzzzzz".getBytes());
    }

    @Test
    void testPrefixScan02() {
        storeInstance.upsertKeyValue("dddd1000".getBytes(), "value01".getBytes());
        storeInstance.upsertKeyValue("ccbb10001".getBytes(), "value02".getBytes());
        storeInstance.upsertKeyValue("dddd20002".getBytes(), "value03".getBytes());
        storeInstance.upsertKeyValue("dddb100043".getBytes(), "value04".getBytes());

        Iterator<KeyValue> it = storeInstance.keyValuePrefixScan("ddd".getBytes());
        List<String> result = readIterator(it);
        String[] actual = result.toArray(new String[result.size()]);

        String[] expected = {"value04", "value01", "value03"};
        Assertions.assertArrayEquals(expected, actual);

        storeInstance.delete("0".getBytes(), "zzzzzzzzzzzzz".getBytes());
    }

    @Test
    void testPrefixScan03() {
        storeInstance.upsertKeyValue("eeee1000".getBytes(), "value01".getBytes());
        storeInstance.upsertKeyValue("fbbb10001".getBytes(), "value02".getBytes());
        storeInstance.upsertKeyValue("eeee20002".getBytes(), "value03".getBytes());
        storeInstance.upsertKeyValue("fffb100043".getBytes(), "value04".getBytes());

        Iterator<KeyValue> it = storeInstance.keyValuePrefixScan("eeee1".getBytes());
        List<String> result = readIterator(it);
        String[] actual = result.toArray(new String[result.size()]);

        String[] expected = {"value01"};
        Assertions.assertArrayEquals(expected, actual);

        storeInstance.delete("0".getBytes(), "zzzzzzzzzzzzz".getBytes());
    }

    @Disabled
    @Test
    void testDelete01() {
        storeInstance.upsertKeyValue("oaaa1000".getBytes(), "value01".getBytes());

        Assertions.assertTrue(storeInstance.delete("oaaa1000".getBytes()));
    }

    @Disabled
    @Test
    void testDeleteRange01() {
        storeInstance.upsertKeyValue("vaaa1000".getBytes(), "value01".getBytes());
        storeInstance.upsertKeyValue("vbbb10001".getBytes(), "value02".getBytes());
        storeInstance.upsertKeyValue("vvee20002".getBytes(), "value03".getBytes());
        storeInstance.upsertKeyValue("vffb100043".getBytes(), "value04".getBytes());

        Assertions.assertTrue(storeInstance.delete("vaaa1000".getBytes(), "vffb100043".getBytes()));

        Iterator<KeyValue> it = storeInstance.keyValueScan("vaaa1000".getBytes(), "vffb100043".getBytes());
        List<String> result = readIterator(it);
        Assertions.assertEquals(0, result.size());

        storeInstance.delete("0".getBytes(), "zzzzzzzzzzzzz".getBytes());
    }

    @Disabled
    @Test
    void testDeleteRange02() {
        storeInstance.upsertKeyValue("waaa1000".getBytes(), "value01".getBytes());
        storeInstance.upsertKeyValue("wbbb10001".getBytes(), "value02".getBytes());
        storeInstance.upsertKeyValue("wvee20002".getBytes(), "value03".getBytes());
        storeInstance.upsertKeyValue("wffb100043".getBytes(), "value04".getBytes());

        Assertions.assertTrue(storeInstance.delete("000000".getBytes(), "zzzzzz".getBytes()));

        Iterator<KeyValue> it = storeInstance.keyValueScan("000000".getBytes(), "zzzzzz".getBytes());
        List<String> result = readIterator(it);
        Assertions.assertEquals(0, result.size());

        storeInstance.delete("0".getBytes(), "zzzzzzzzzzzzz".getBytes());
    }
}
