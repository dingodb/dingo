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

package io.dingodb.serial.test.io;

import io.dingodb.serial.io.RecordDecoder;
import io.dingodb.serial.io.RecordEncoder;
import io.dingodb.serial.schema.BooleanListSchema;
import io.dingodb.serial.schema.BooleanSchema;
import io.dingodb.serial.schema.DingoSchema;
import io.dingodb.serial.schema.DoubleListSchema;
import io.dingodb.serial.schema.DoubleSchema;
import io.dingodb.serial.schema.FloatListSchema;
import io.dingodb.serial.schema.FloatSchema;
import io.dingodb.serial.schema.IntegerListSchema;
import io.dingodb.serial.schema.IntegerSchema;
import io.dingodb.serial.schema.LongListSchema;
import io.dingodb.serial.schema.LongSchema;
import io.dingodb.serial.schema.ShortListSchema;
import io.dingodb.serial.schema.ShortSchema;
import io.dingodb.serial.schema.StringListSchema;
import io.dingodb.serial.schema.StringSchema;
import io.dingodb.serial.util.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class RecordTest {
    @Test
    public void testRecord() throws Exception {
        List<DingoSchema> schemas = new ArrayList<>();

        schemas.add(new BooleanSchema(0));
        schemas.add(new BooleanSchema(1));
        schemas.add(new BooleanSchema(2, false));
        schemas.add(new BooleanSchema(3, true));
        schemas.add(new StringSchema(4,0));
        schemas.add(new StringSchema(5, 0, "testnull1"));
        schemas.add(new StringSchema(6, 0, "testnull2"));
        schemas.add(new ShortSchema(7));
        schemas.add(new IntegerSchema(8));
        schemas.add(new FloatSchema(9));
        schemas.add(new LongSchema(10));
        schemas.add(new DoubleSchema(11));

        Object[] record = new Object[12];
        record[0] = true;
        record[1] = null;
        record[2] = true;
        record[3] = null;
        record[4] = "test string 1";
        record[5] = "";
        record[6] = null;
        record[7] = (short) 1;
        record[8] = 1;
        record[9] = 1f;
        record[10] = 1L;
        record[11] = 1d;

        RecordEncoder re = new RecordEncoder(schemas, (short) 0, (byte) 1, (byte) 1, (byte) 1, null);
        byte[] result = re.encode(record);

        RecordDecoder rd = new RecordDecoder(schemas, (short) 0, (byte) 1, (byte) 1, (byte) 1, null);
        Object[] recordDe = rd.decode(result);

        record[3] = true;
        record[6] = "testnull2";

        Assertions.assertArrayEquals(record, recordDe);

        Assertions.assertEquals(rd.decode(result, new int[]{3})[0], true);
        Assertions.assertEquals(rd.decode(result, new int[]{5})[0], "");
        Assertions.assertEquals(rd.decode(result, new int[]{6})[0], "testnull2");

        Assertions.assertEquals(rd.decode(result, new int[]{3,5,6})[0], true);
        Assertions.assertEquals(rd.decode(result, new int[]{5,3,6})[0], "");
        Assertions.assertEquals(rd.decode(result, new int[]{6,3,5})[0], "testnull2");
        Assertions.assertEquals(rd.decode(result, new int[]{6,3,5})[1], true);
        Assertions.assertEquals(rd.decode(result, new int[]{6,3,5})[2], "");



        re.encode(result, new int[]{1}, new Object[]{false});
        recordDe = rd.decode(result);
        Assertions.assertEquals(recordDe[1], false);
        Assertions.assertEquals(recordDe[11], 1d);

        result = re.encode(result, new int[]{1}, new Object[]{false});
        recordDe = rd.decode(result);
        Assertions.assertEquals(recordDe[1], false);
        Assertions.assertEquals(recordDe[11], 1d);

        re.encode(result, new int[]{1}, new Object[]{null});
        recordDe = rd.decode(result);
        Assertions.assertEquals(recordDe[1], null);

        re.encode(result, new int[]{2}, new Object[]{null});
        recordDe = rd.decode(result);
        Assertions.assertEquals(recordDe[2], false);

        result = re.encode(result, new int[]{4}, new Object[]{null});
        recordDe = rd.decode(result);
        Assertions.assertEquals(recordDe[4], null);

        result = re.encode(result, new int[]{5}, new Object[]{null});
        recordDe = rd.decode(result);
        Assertions.assertEquals(recordDe[5], "testnull1");
        Assertions.assertEquals(recordDe[11], 1d);

        result = re.encode(result, new int[]{5}, new Object[]{"测试测试测试三🤣😂😁🐱‍🐉👏🐱‍💻✔🤳🤦‍♂️🤦‍♀️🙌测试测试测试测试测试Iv2===v32"
            + "伍仟肆佰伍拾叁亿肆仟伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫但是超长字符串哈哈吃的食堂的饭"});
        recordDe = rd.decode(result);
        Assertions.assertEquals(recordDe[5], "测试测试测试三🤣😂😁🐱‍🐉👏🐱‍💻✔🤳🤦‍♂️🤦‍♀️🙌测试测试测试测试测试Iv2===v32"
            + "伍仟肆佰伍拾叁亿肆仟伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫但是超长字符串哈哈吃的食堂的饭");

        result = re.encode(result, new int[]{6}, new Object[]{null});
        recordDe = rd.decode(result);
        Assertions.assertEquals(recordDe[6], "testnull2");

        result = re.encode(result, new int[]{6}, new Object[]{"1"});
        recordDe = rd.decode(result);
        Assertions.assertEquals(recordDe[6], "1");
        Assertions.assertEquals(recordDe[5], "测试测试测试三🤣😂😁🐱‍🐉👏🐱‍💻✔🤳🤦‍♂️🤦‍♀️🙌测试测试测试测试测试Iv2===v32"
            + "伍仟肆佰伍拾叁亿肆仟伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫但是超长字符串哈哈吃的食堂的饭");
        Assertions.assertEquals(recordDe[4], null);


        result = re.encode(result, new int[]{6, 5, 4, 3}, new Object[]{null, "test5", null, true});
        recordDe = rd.decode(result);
        Assertions.assertEquals(recordDe[3], true);
        Assertions.assertEquals(recordDe[4], null);
        Assertions.assertEquals(recordDe[5], "test5");
        Assertions.assertEquals(recordDe[6], "testnull2");
    }

    @Test
    public void testKeyRecord() throws Exception {
        List<DingoSchema> schemas = new ArrayList<>();

        schemas.add(new BooleanSchema(0));
        schemas.add(new BooleanSchema(1));
        schemas.add(new BooleanSchema(2, false));
        schemas.add(new BooleanSchema(3, true));
        schemas.add(new StringSchema(4,0));
        schemas.add(new StringSchema(5, 0, "testnull1"));
        schemas.add(new StringSchema(6, 0, "testnull2"));
        schemas.add(new ShortSchema(7));
        schemas.add(new IntegerSchema(8));
        schemas.add(new FloatSchema(9));
        schemas.add(new LongSchema(10));
        schemas.add(new DoubleSchema(11));

        Object[] record = new Object[12];
        record[0] = true;
        record[1] = null;
        record[2] = true;
        record[3] = null;
        record[4] = "test string 1";
        record[5] = "";
        record[6] = null;
        record[7] = (short) 1;
        record[8] = 1;
        record[9] = 1f;
        record[10] = 1L;
        record[11] = 1d;

        RecordEncoder re = new RecordEncoder(schemas, (short) 0, (byte) 1, (byte) 1, (byte) 1, null);
        byte[] result = re.encodeKey(record);

        RecordDecoder rd = new RecordDecoder(schemas, (short) 0, (byte) 1, (byte) 1, (byte) 1, null);
        Object[] recordDe = rd.decodeKey(result);

        record[3] = true;
        record[6] = "testnull2";

        Assertions.assertArrayEquals(record, recordDe);

        Assertions.assertEquals(rd.decodeKey(result, new int[]{3})[0], true);
        Assertions.assertEquals(rd.decodeKey(result, new int[]{5})[0], "");
        Assertions.assertEquals(rd.decodeKey(result, new int[]{6})[0], "testnull2");

        Assertions.assertEquals(rd.decodeKey(result, new int[]{3,5,6})[0], true);
        Assertions.assertEquals(rd.decodeKey(result, new int[]{5,3,6})[0], "");
        Assertions.assertEquals(rd.decodeKey(result, new int[]{6,3,5})[0], "testnull2");
        Assertions.assertEquals(rd.decodeKey(result, new int[]{6,3,5})[1], true);
        Assertions.assertEquals(rd.decodeKey(result, new int[]{6,3,5})[2], "");



        result = re.encodeKey(result, new int[]{1}, new Object[]{false});
        recordDe = rd.decodeKey(result);
        Assertions.assertEquals(recordDe[1], false);
        Assertions.assertEquals(recordDe[11], 1d);

        result = re.encodeKey(result, new int[]{1}, new Object[]{false});
        recordDe = rd.decodeKey(result);
        Assertions.assertEquals(recordDe[1], false);
        Assertions.assertEquals(recordDe[11], 1d);

        result = re.encodeKey(result, new int[]{1}, new Object[]{null});
        recordDe = rd.decodeKey(result);
        Assertions.assertEquals(recordDe[1], null);

        result = re.encodeKey(result, new int[]{2}, new Object[]{null});
        recordDe = rd.decodeKey(result);
        Assertions.assertEquals(recordDe[2], false);

        result = re.encodeKey(result, new int[]{4}, new Object[]{null});
        recordDe = rd.decodeKey(result);
        Assertions.assertEquals(recordDe[4], null);

        result = re.encodeKey(result, new int[]{5}, new Object[]{null});
        recordDe = rd.decodeKey(result);
        Assertions.assertEquals(recordDe[5], "testnull1");
        Assertions.assertEquals(recordDe[11], 1d);

        result = re.encodeKey(result, new int[]{5}, new Object[]{"测试测试测试三🤣😂😁🐱‍🐉👏🐱‍💻✔🤳🤦‍♂️🤦‍♀️🙌测试测试测试测试测试Iv2===v32"
            + "伍仟肆佰伍拾叁亿肆仟伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫但是超长字符串哈哈吃的食堂的饭"});
        recordDe = rd.decodeKey(result);
        Assertions.assertEquals(recordDe[5], "测试测试测试三🤣😂😁🐱‍🐉👏🐱‍💻✔🤳🤦‍♂️🤦‍♀️🙌测试测试测试测试测试Iv2===v32"
            + "伍仟肆佰伍拾叁亿肆仟伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫但是超长字符串哈哈吃的食堂的饭");

        result = re.encodeKey(result, new int[]{6}, new Object[]{null});
        recordDe = rd.decodeKey(result);
        Assertions.assertEquals(recordDe[6], "testnull2");

        result = re.encodeKey(result, new int[]{6}, new Object[]{"1"});
        recordDe = rd.decodeKey(result);
        Assertions.assertEquals(recordDe[6], "1");
        Assertions.assertEquals(recordDe[5], "测试测试测试三🤣😂😁🐱‍🐉👏🐱‍💻✔🤳🤦‍♂️🤦‍♀️🙌测试测试测试测试测试Iv2===v32"
            + "伍仟肆佰伍拾叁亿肆仟伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫但是超长字符串哈哈吃的食堂的饭");
        Assertions.assertEquals(recordDe[4], null);


        result = re.encodeKey(result, new int[]{6, 5, 4, 3}, new Object[]{null, "test5", null, true});
        recordDe = rd.decodeKey(result);
        Assertions.assertEquals(recordDe[3], true);
        Assertions.assertEquals(recordDe[4], null);
        Assertions.assertEquals(recordDe[5], "test5");
        Assertions.assertEquals(recordDe[6], "testnull2");
    }

    @Test
    public void testListRecord() throws Exception {
        List<DingoSchema> schemas = new ArrayList<>();
        BooleanListSchema booleanListSchema = new BooleanListSchema(0);
        ShortListSchema shortListSchema = new ShortListSchema(1);
        IntegerListSchema integerListSchema = new IntegerListSchema(2);
        FloatListSchema floatListSchema = new FloatListSchema(3);
        LongListSchema longListSchema = new LongListSchema(4);
        DoubleListSchema doubleListSchema = new DoubleListSchema(5);
        StringListSchema stringListSchema = new StringListSchema(6);

        IntegerSchema integerSchema = new IntegerSchema(7);

        schemas.add(booleanListSchema);
        schemas.add(shortListSchema);
        schemas.add(integerListSchema);
        schemas.add(floatListSchema);
        schemas.add(longListSchema);
        schemas.add(doubleListSchema);
        schemas.add(stringListSchema);
        schemas.add(integerSchema);

        Utils.sortSchema(schemas);

        Object[] record = new Object[8];

        List<Boolean> booleans = new ArrayList<>();
        booleans.add(true);
        booleans.add(false);
        record[0] = booleans;

        List<Short> shorts = new ArrayList<>();
        shorts.add((short) 0);
        shorts.add((short) 1);
        shorts.add((short) 2);
        record[1] = shorts;

        List<Integer> integers = new ArrayList<>();
        integers.add(0);
        integers.add(1);
        integers.add(2);
        record[2] = integers;

        List<Float> floats = new ArrayList<>();
        floats.add(0f);
        floats.add(1f);
        floats.add(2f);
        record[3] = floats;

        List<Long> longs = new ArrayList<>();
        longs.add(0L);
        longs.add(1L);
        longs.add(2L);
        record[4] = longs;

        List<Double> doubles = new ArrayList<>();
        doubles.add(0d);
        doubles.add(1d);
        doubles.add(2d);
        record[5] = doubles;

        List<String> strings = new ArrayList<>();
        strings.add("0");
        strings.add("1");
        strings.add("2");
        record[6] = strings;

        record[7] = 0;

        RecordEncoder re = new RecordEncoder(schemas, (short) 0, (byte) 1, (byte) 1, (byte) 1, null);
        byte[] result = re.encode(record);

        RecordDecoder rd = new RecordDecoder(schemas, (short) 0, (byte) 1, (byte) 1, (byte) 1, null);
        Object[] recordDe = rd.decode(result);

        List<Boolean> booleans1 = (List<Boolean>) recordDe[0];
        List<Short> shorts1 = (List<Short>) recordDe[1];
        List<Integer> integers1 = (List<Integer>) recordDe[2];
        List<Float> floats1 = (List<Float>) recordDe[3];
        List<Long> longs1 = (List<Long>) recordDe[4];
        List<Double> doubles1 = (List<Double>) recordDe[5];
        List<String> strings1 = (List<String>) recordDe[6];
        Integer integer = (Integer) recordDe[7];

        Assertions.assertEquals(booleans.get(0), booleans1.get(0));
        Assertions.assertEquals(booleans.get(1), booleans1.get(1));
        for (int i = 0; i < 3; i++) {
            Assertions.assertEquals(shorts.get(i), shorts1.get(i));
            Assertions.assertEquals(integers.get(i), integers1.get(i));
            Assertions.assertEquals(floats.get(i), floats1.get(i));
            Assertions.assertEquals(longs.get(i), longs1.get(i));
            Assertions.assertEquals(doubles.get(i), doubles1.get(i));
            Assertions.assertEquals(strings.get(i), strings1.get(i));
        }
        Assertions.assertEquals(integer, 0);

        result = re.encode(result, new int[] {7}, new Object[] {1});
        recordDe = rd.decode(result);

        booleans1 = (List<Boolean>) recordDe[0];
        shorts1 = (List<Short>) recordDe[1];
        integers1 = (List<Integer>) recordDe[2];
        floats1 = (List<Float>) recordDe[3];
        longs1 = (List<Long>) recordDe[4];
        doubles1 = (List<Double>) recordDe[5];
        strings1 = (List<String>) recordDe[6];
        integer = (Integer) recordDe[7];

        Assertions.assertEquals(booleans.get(0), booleans1.get(0));
        Assertions.assertEquals(booleans.get(1), booleans1.get(1));
        for (int i = 0; i < 3; i++) {
            Assertions.assertEquals(shorts.get(i), shorts1.get(i));
            Assertions.assertEquals(integers.get(i), integers1.get(i));
            Assertions.assertEquals(floats.get(i), floats1.get(i));
            Assertions.assertEquals(longs.get(i), longs1.get(i));
            Assertions.assertEquals(doubles.get(i), doubles1.get(i));
            Assertions.assertEquals(strings.get(i), strings1.get(i));
        }
        Assertions.assertEquals(integer, 1);

        integers.add(4);
        strings.add("4");


        result = re.encode(result, new int[] {2,6,7}, new Object[] {integers, strings, 2});
        recordDe = rd.decode(result);

        booleans1 = (List<Boolean>) recordDe[0];
        shorts1 = (List<Short>) recordDe[1];
        integers1 = (List<Integer>) recordDe[2];
        floats1 = (List<Float>) recordDe[3];
        longs1 = (List<Long>) recordDe[4];
        doubles1 = (List<Double>) recordDe[5];
        strings1 = (List<String>) recordDe[6];
        integer = (Integer) recordDe[7];

        Assertions.assertEquals(booleans.get(0), booleans1.get(0));
        Assertions.assertEquals(booleans.get(1), booleans1.get(1));
        for (int i = 0; i < 3; i++) {
            Assertions.assertEquals(shorts.get(i), shorts1.get(i));
            Assertions.assertEquals(integers.get(i), integers1.get(i));
            Assertions.assertEquals(floats.get(i), floats1.get(i));
            Assertions.assertEquals(longs.get(i), longs1.get(i));
            Assertions.assertEquals(doubles.get(i), doubles1.get(i));
            Assertions.assertEquals(strings.get(i), strings1.get(i));
        }
        Assertions.assertEquals(integers.get(3), integers1.get(3));
        Assertions.assertEquals(strings.get(3), strings1.get(3));
        Assertions.assertEquals(integer, 2);
    }
}
