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

package io.dingodb.serial.test.util;

import io.dingodb.serial.schema.BooleanSchema;
import io.dingodb.serial.schema.DingoSchema;
import io.dingodb.serial.schema.DoubleSchema;
import io.dingodb.serial.schema.FloatSchema;
import io.dingodb.serial.schema.IntegerSchema;
import io.dingodb.serial.schema.LongSchema;
import io.dingodb.serial.schema.ShortSchema;
import io.dingodb.serial.schema.StringSchema;
import io.dingodb.serial.util.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class UtilsTest {

    @Test
    public void testSort() {
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
        schemas.add(new StringSchema(12, 5));

        Utils.sortSchema(schemas);

        Assertions.assertEquals(schemas.get(0).getIndex(), 0);
        Assertions.assertEquals(schemas.get(1).getIndex(), 1);
        Assertions.assertEquals(schemas.get(2).getIndex(), 2);
        Assertions.assertEquals(schemas.get(3).getIndex(), 3);
        Assertions.assertEquals(schemas.get(4).getIndex(), 11);
        Assertions.assertEquals(schemas.get(5).getIndex(), 10);
        Assertions.assertEquals(schemas.get(6).getIndex(), 9);
        Assertions.assertEquals(schemas.get(7).getIndex(), 7);
        Assertions.assertEquals(schemas.get(8).getIndex(), 8);
        Assertions.assertEquals(schemas.get(9).getIndex(), 6);
        Assertions.assertEquals(schemas.get(10).getIndex(), 5);
        Assertions.assertEquals(schemas.get(11).getIndex(), 4);
        Assertions.assertEquals(schemas.get(12).getIndex(), 12);

        Utils.sortSchema(schemas);

        Assertions.assertEquals(schemas.get(0).getIndex(), 0);
        Assertions.assertEquals(schemas.get(1).getIndex(), 1);
        Assertions.assertEquals(schemas.get(2).getIndex(), 2);
        Assertions.assertEquals(schemas.get(3).getIndex(), 3);
        Assertions.assertEquals(schemas.get(4).getIndex(), 11);
        Assertions.assertEquals(schemas.get(5).getIndex(), 10);
        Assertions.assertEquals(schemas.get(6).getIndex(), 9);
        Assertions.assertEquals(schemas.get(7).getIndex(), 7);
        Assertions.assertEquals(schemas.get(8).getIndex(), 8);
        Assertions.assertEquals(schemas.get(9).getIndex(), 6);
        Assertions.assertEquals(schemas.get(10).getIndex(), 5);
        Assertions.assertEquals(schemas.get(11).getIndex(), 4);
        Assertions.assertEquals(schemas.get(12).getIndex(), 12);

        Utils.sortSchema(schemas);

        Assertions.assertEquals(schemas.get(0).getIndex(), 0);
        Assertions.assertEquals(schemas.get(1).getIndex(), 1);
        Assertions.assertEquals(schemas.get(2).getIndex(), 2);
        Assertions.assertEquals(schemas.get(3).getIndex(), 3);
        Assertions.assertEquals(schemas.get(4).getIndex(), 11);
        Assertions.assertEquals(schemas.get(5).getIndex(), 10);
        Assertions.assertEquals(schemas.get(6).getIndex(), 9);
        Assertions.assertEquals(schemas.get(7).getIndex(), 7);
        Assertions.assertEquals(schemas.get(8).getIndex(), 8);
        Assertions.assertEquals(schemas.get(9).getIndex(), 6);
        Assertions.assertEquals(schemas.get(10).getIndex(), 5);
        Assertions.assertEquals(schemas.get(11).getIndex(), 4);
        Assertions.assertEquals(schemas.get(12).getIndex(), 12);
    }

    @Test
    public void testApproSize() {
        List<DingoSchema> schemas = new ArrayList<>();

        schemas.add(new BooleanSchema(0));  //2
        schemas.add(new BooleanSchema(1));  //2
        schemas.add(new BooleanSchema(2, false));  //2
        schemas.add(new BooleanSchema(3, true));   //2
        schemas.add(new StringSchema(4,0));  //100
        schemas.add(new StringSchema(5, 0, "testnull1"));  //100
        schemas.add(new StringSchema(6, 0, "testnull2"));  //100
        schemas.add(new ShortSchema(7));  //3
        schemas.add(new IntegerSchema(8));  //5
        schemas.add(new FloatSchema(9));  //5
        schemas.add(new LongSchema(10));  //9
        schemas.add(new DoubleSchema(11));  //9
        schemas.add(new StringSchema(12, 5));  //10

        Assertions.assertEquals(Utils.getApproPerRecordSize(schemas)[0], 349);
    }
}
