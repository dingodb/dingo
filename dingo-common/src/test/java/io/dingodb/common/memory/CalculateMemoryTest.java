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

package io.dingodb.common.memory;

import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.ClassData;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.layouters.CurrentLayouter;

import java.math.BigDecimal;
import java.util.UUID;

public class CalculateMemoryTest {
    public static void main(String[] args) {
        //testString();
        //testTuples();
    }

    private static ClassLayout parseInstance(Object instance) {
        return new CurrentLayouter().layout(ClassData.parseInstance(instance));
    }


    public static long calculateObjectSize(Object object) {
        ClassLayout layout = parseInstance(object);
        return layout.instanceSize();
    }

    @Test
    public void testString() {
        Runtime runtime = Runtime.getRuntime();
        long before = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("before:" + before);
        long a = 0;
        for (int i = 0; i < 10000; i ++) {
            String item = UUID.randomUUID().toString();
            long itemSize = ObjectSizeUtils.calculateDataSize(item);
            a += itemSize;
        }
        System.out.println("string calculate:" + a);
        long after = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("after:" + after);
        System.out.println("after string size:" + (after - before));

    }

    @Test
    public void testTuples() {
        Runtime runtime = Runtime.getRuntime();
        long before = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("before:" + before);
        long size = 0;
        for (int i = 0; i < 50000; i ++) {
            Object[] tuples = new Object[10];
            tuples[0] = 0;
            tuples[1] = "aa";
            tuples[2] = 2L;
            tuples[3] = 3D;
            tuples[4] = 4F;
            tuples[5] = new BigDecimal("11");

            size += tuples.length * ObjectSizeUtils.REFERENCE_SIZE;
            for (Object data : tuples) {
                size += ObjectSizeUtils.calculateDataSize(data);
            }
        }
        System.out.println("tuple calculate:" + size);
        long after = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("after:" + after);
        System.out.println("tuple size:" + (after - before));
    }


}
