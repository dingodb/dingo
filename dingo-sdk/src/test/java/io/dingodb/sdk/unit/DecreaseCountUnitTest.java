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
import io.dingodb.sdk.operation.unit.numeric.IncreaseCountUnit;
import io.dingodb.sdk.operation.unit.numeric.MaxDecreaseCountUnit;
import io.dingodb.sdk.operation.unit.numeric.NumberUnit;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DecreaseCountUnitTest {

    @Test
    public void testLong01() {
        String col = "test";
        Map<String, NumberUnit> map = new HashMap<>();
        Iterator<Integer> records = Lists.newArrayList(77, 78, 77, 76, 75, 100, 80, 79, 78, 67).iterator();
        while (records.hasNext()) {
            Integer record = records.next();
            NumberUnit unit = new DecreaseCountUnit<>(ComputeInteger.of(record));
            map.merge(col, unit, NumberUnit::merge);
        }

        System.out.println(map.get(col).value());

    }
}
