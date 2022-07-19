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
import io.dingodb.serial.schema.*;
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

        RecordEncoder re = new RecordEncoder(schemas, (short) 0);
        byte[] result = re.encode(record);

        RecordDecoder rd = new RecordDecoder(schemas, (short) 0);
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
}
