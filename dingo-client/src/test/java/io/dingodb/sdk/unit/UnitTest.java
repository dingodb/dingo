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

package io.dingodb.sdk.unit;

import com.google.common.collect.Lists;
import io.dingodb.sdk.operation.number.ComputeInteger;
import io.dingodb.sdk.operation.unit.numeric.DecreaseCountUnit;
import io.dingodb.sdk.operation.unit.numeric.MaxContinuousCountUnit;
import io.dingodb.sdk.operation.unit.numeric.NumberUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class UnitTest {

    @Test
    public void testDecrease() {
        String col = "test";
        Map<String, NumberUnit> map = new HashMap<>();
        Iterator<Integer> records = Lists.newArrayList(77, 78, 77, 76, 75, 100, 80, 79, 78, 67).iterator();
        while (records.hasNext()) {
            Integer record = records.next();
            NumberUnit unit = new DecreaseCountUnit<>(ComputeInteger.of(record));
            map.merge(col, unit, NumberUnit::merge);
        }

        Assertions.assertEquals(map.get(col).value(), 7L);

    }

    @Test
    public void testMaxContinuousDecrease() {
        String col = "test";
        Map<String, NumberUnit> map = new HashMap<>();
        Iterator<Integer> records = Lists.newArrayList(77, 78, 77, 76, 75, 100, 80, 79, 78, 67).iterator();
        Integer center = 0;
        while (records.hasNext()) {
            Integer record = records.next();
            if (map.get(col) == null) {
                MaxContinuousCountUnit unit = new MaxContinuousCountUnit(false);
                map.put(col, unit);
                center = record;
                continue;
            }
            MaxContinuousCountUnit unit = new MaxContinuousCountUnit(record < center);
            center = record;
            map.merge(col, unit, NumberUnit::merge);
        }

        Assertions.assertEquals(map.get(col).value(), 4L);
    }
}
