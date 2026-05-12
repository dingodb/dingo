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

package io.dingodb.calcite;

import io.dingodb.calcite.meta.DingoSortedMultiMap;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

public class SortedMultiMapTest {
    @Test
    public void putMulti() {
        DingoSortedMultiMap dingoSortedMultiMap = new DingoSortedMultiMap();
        Random random = new Random();
        for (int i = 0; i < 10000; i ++) {
            int randomVal = random.nextInt(1000);
            dingoSortedMultiMap.putMulti(random.nextInt(1000), new Object[]{randomVal,2,2,3,3,2,"12"});
        }
        System.out.println("map key size:" + dingoSortedMultiMap.size());
        Iterator iterator = dingoSortedMultiMap.arrays(new Comparator() {
            @Override
            public int compare(Object o, Object t1) {
                return compare((Object[])o, (Object[])t1);
            }

            public int compare(Object[] v0, Object[] v1) {
                Object p1 = v0[0];
                Object p2 = v1[0];
                if (p1 == null || p2 == null) {
                    return 0;
                } else {
                    return (Integer) p1 - (Integer) p2;
                }
            }
        });

        while (iterator.hasNext()) {
            Object[] it = (Object[]) iterator.next();
            for (Object subItem : it) {
                Object[] objects = (Object[]) subItem;
                System.out.println("-->" + objects[0]);
            }
            System.out.println("-----------");
        }

    }
}
