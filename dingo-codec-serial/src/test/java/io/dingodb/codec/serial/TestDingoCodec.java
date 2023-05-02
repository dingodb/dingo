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

package io.dingodb.codec.serial;

import io.dingodb.codec.Codec;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestDingoCodec {
    private static TableDefinition tableDefinition;

    private static KeyValueCodec codec;

    private final Object[] record = new Object[]{1, "name", 0.5d};

    @BeforeAll
    public static void setupAll() throws IOException {
        tableDefinition = TableDefinition.readJson(
            TestDingoCodec.class.getResourceAsStream("/table-test.json")
        );
        codec =
            new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
    }

    @Test
    public void testRecord() throws IOException {
        KeyValue keyValue = codec.encode(record);
        Object[] result = codec.decode(keyValue);
        Assertions.assertArrayEquals(result, record);
    }

    @Test
    public void testKey() throws IOException {
        Object[] keys = tableDefinition.getKeyMapping().revMap(record);
        byte[] key = codec.encodeKey(keys);
        KeyValue keyValue = codec.encode(record);
        Assertions.assertArrayEquals(key, keyValue.getKey());
    }

    @Test
    public void testValue() throws IOException {
        KeyValue keyValue = codec.encode(record);
        Object[] key = new Object[]{record[0]};
        Object[] result = codec.mapKeyAndDecodeValue(key, keyValue.getValue());
        Assertions.assertArrayEquals(result, record);
    }

    @Test
    public void testValueSelect() throws IOException {
        KeyValue keyValue = codec.encode(record);
        Codec valueCodec = new DingoCodec(tableDefinition.getDingoSchemaOfValue());
        Object[] value1 = valueCodec.decode(keyValue.getValue(), new int[]{1});
        Assertions.assertEquals(value1[0], record[2]);

        Object[] value01 = valueCodec.decode(keyValue.getValue(), new int[]{0, 1});
        Assertions.assertEquals(value01[0], record[1]);
        Assertions.assertEquals(value01[1], record[2]);
    }

    @Test
    public void testValueUpdate() throws IOException {
        KeyValue keyValue = codec.encode(record);
        Codec valueCodec = new DingoCodec(tableDefinition.getDingoSchemaOfValue());
        byte[] updated1 = valueCodec.encode(keyValue.getValue(), new Object[]{1d}, new int[]{1});
        Object[] result1 = valueCodec.decode(updated1);
        Assertions.assertEquals(result1[0], record[1]);
        Assertions.assertEquals(result1[1], 1d);

        byte[] updated2 = valueCodec.encode(keyValue.getValue(), new Object[]{"test", 1d}, new int[]{0, 1});
        Object[] result2 = valueCodec.decode(updated2);
        Assertions.assertEquals(result2[0], "test");
        Assertions.assertEquals(result2[1], 1d);
    }
}
