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

import io.dingodb.serial.io.BinaryDecoder;
import io.dingodb.serial.io.BinaryEncoder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BinaryTest {

    @Test
    public void test() {
        BinaryEncoder be = new BinaryEncoder(new byte[80]);

        be.writeBoolean(false); //length 2
        be.writeBoolean(null);
        be.writeShort((short) 1); //length 3
        be.writeShort(null);
        be.writeInt(1); //length 5
        be.writeInt(null);
        be.writeLong(1L); //length 9
        be.writeLong(null);
        be.writeFloat(1f); //length 5
        be.writeFloat(null);
        be.writeDouble(1d); //length 9
        be.writeDouble(null);
        be.writeString("123", 0);
        be.writeString("测试长度超过80的情况", 0);
        be.writeString("", 0);
        be.writeString(null, 0);

        byte[] result = be.getByteArray();

        BinaryDecoder bd = new BinaryDecoder(result);
        assertEquals(bd.readBoolean(), false);
        assertEquals(bd.readBoolean(), null);
        assertEquals(bd.readShort(), (short) 1);
        assertEquals(bd.readShort(), null);
        assertEquals(bd.readInt(), 1);
        assertEquals(bd.readInt(), null);
        assertEquals(bd.readLong(), 1L);
        assertEquals(bd.readLong(), null);
        assertEquals(bd.readFloat(), 1f);
        assertEquals(bd.readFloat(), null);
        assertEquals(bd.readDouble(), 1d);
        assertEquals(bd.readDouble(), null);
        assertEquals(bd.readString(), "123");
        assertEquals(bd.readString(), "测试长度超过80的情况");
        assertEquals(bd.readString(), "");
        assertEquals(bd.readString(), null);
        assertEquals(bd.remainder(), 0);
    }

    @Test
    public void testList() {
        BinaryEncoder be = new BinaryEncoder(1);
        List<Boolean> booleanList1 = new ArrayList<>();
        booleanList1.add(true);
        booleanList1.add(true);
        booleanList1.add(false);
        booleanList1.add(false);
        be.writeBooleanList(booleanList1);

        List<Boolean> booleanList2 = new ArrayList<>();
        booleanList2.add(true);
        booleanList2.add(true);
        booleanList2.add(true);
        booleanList2.add(false);
        be.writeBooleanList(booleanList2);

        List<Boolean> booleanList3 = new ArrayList<>();
        booleanList3.add(true);
        booleanList3.add(true);
        booleanList3.add(true);
        booleanList3.add(true);
        be.writeBooleanList(booleanList3);

        byte[] record = be.getByteArray();


        BinaryDecoder bd = new BinaryDecoder(record);
        List<Boolean> l1 = bd.readBooleanList();
        List<Boolean> l2 = bd.readBooleanList();
        List<Boolean> l3 = bd.readBooleanList();

        assertEquals(l1.size(), 4);
        assertEquals(l1.get(0), true);
        assertEquals(l1.get(1), true);
        assertEquals(l1.get(2), false);
        assertEquals(l1.get(3), false);

        assertEquals(l2.size(), 4);
        assertEquals(l2.get(0), true);
        assertEquals(l2.get(1), true);
        assertEquals(l2.get(2), true);
        assertEquals(l2.get(3), false);

        assertEquals(l3.size(), 4);
        assertEquals(l3.get(0), true);
        assertEquals(l3.get(1), true);
        assertEquals(l3.get(2), true);
        assertEquals(l3.get(3), true);

        BinaryEncoder be2 = new BinaryEncoder(record);
        be2.skipBooleanList();
        be2.updateBooleanList(null);
        be2.skipBooleanList();
        record = be2.getByteArray();
        BinaryDecoder bd2 = new BinaryDecoder(record);

        l1 = bd2.readBooleanList();
        l2 = bd2.readBooleanList();
        l3 = bd2.readBooleanList();

        assertEquals(l1.size(), 4);
        assertEquals(l1.get(0), true);
        assertEquals(l1.get(1), true);
        assertEquals(l1.get(2), false);
        assertEquals(l1.get(3), false);

        assertEquals(l2, null);

        assertEquals(l3.size(), 4);
        assertEquals(l3.get(0), true);
        assertEquals(l3.get(1), true);
        assertEquals(l3.get(2), true);
        assertEquals(l3.get(3), true);


        List<Short> shortList = new ArrayList<>();
        shortList.add((short) 1);
        shortList.add((short) 2);
        shortList.add((short) 3);

        List<Integer> intList = new ArrayList<>();
        intList.add(1);
        intList.add(2);
        intList.add(3);

        List<Float> floatList = new ArrayList<>();
        floatList.add(1f);
        floatList.add(2f);
        floatList.add(3f);

        List<Long> longList = new ArrayList<>();
        longList.add(1L);
        longList.add(2L);
        longList.add(3L);

        List<Double> doubleList = new ArrayList<>();
        doubleList.add(1D);
        doubleList.add(2D);
        doubleList.add(3D);
        BinaryEncoder be3 = new BinaryEncoder(1);
        be3.writeShortList(shortList);
        be3.writeIntegerList(intList);
        be3.writeFloatList(floatList);
        be3.writeLongList(longList);
        be3.writeDoubleList(doubleList);


        BinaryDecoder bd3 = new BinaryDecoder(be3.getByteArray());

        shortList = bd3.readShortList();
        intList = bd3.readIntegerList();
        floatList = bd3.readFloatList();
        longList = bd3.readLongList();
        doubleList = bd3.readDoubleList();

        assertEquals(shortList.get(2), (short) 3);
        assertEquals(intList.get(2), 3);
        assertEquals(floatList.get(2), 3f);
        assertEquals(longList.get(2), 3L);
        assertEquals(doubleList.get(2), 3D);

        shortList.add((short) 4);
        intList.add(4);
        floatList.add(4f);
        longList.add(4L);
        doubleList.add(4D);

        be3 = new BinaryEncoder(be3.getByteArray());
        be3.updateShortList(shortList);
        be3.updateIntegerList(intList);
        be3.updateFloatList(floatList);
        be3.updateLongList(longList);
        be3.updateDoubleList(doubleList);

        bd3 = new BinaryDecoder(be3.getByteArray());
        shortList = bd3.readShortList();
        intList = bd3.readIntegerList();
        floatList = bd3.readFloatList();
        longList = bd3.readLongList();
        doubleList = bd3.readDoubleList();

        assertEquals(shortList.get(3), (short) 4);
        assertEquals(intList.get(3), 4);
        assertEquals(floatList.get(3), 4f);
        assertEquals(longList.get(3), 4L);
        assertEquals(doubleList.get(3), 4D);
    }

    @Test
    public void testStringList() {
        List<String> s1 = new ArrayList<>();
        s1.add("a1");
        s1.add("b1");
        s1.add("c1");
        List<String> s2 = new ArrayList<>();
        s2.add("a2");
        s2.add("b2");
        s2.add("c2");
        List<String> s3 = new ArrayList<>();
        s3.add("a3");
        s3.add("b3");
        s3.add("c3");
        List<String> s4 = new ArrayList<>();
        s4.add("a4");
        s4.add("b4");
        s4.add("c4");

        BinaryEncoder be1 = new BinaryEncoder(1);
        be1.writeStringList(s1);
        be1.writeStringList(s2);
        be1.writeStringList(s3);
        byte[] record = be1.getByteArray();
        BinaryDecoder bd1 = new BinaryDecoder(record);

        s1 = bd1.readStringList();
        s2 = bd1.readStringList();
        s3 = bd1.readStringList();

        Assertions.assertEquals(s1.get(0), "a1");
        Assertions.assertEquals(s1.get(1), "b1");
        Assertions.assertEquals(s1.get(2), "c1");
        Assertions.assertEquals(s2.get(0), "a2");
        Assertions.assertEquals(s2.get(1), "b2");
        Assertions.assertEquals(s2.get(2), "c2");
        Assertions.assertEquals(s3.get(0), "a3");
        Assertions.assertEquals(s3.get(1), "b3");
        Assertions.assertEquals(s3.get(2), "c3");

        BinaryEncoder be2 = new BinaryEncoder(record);
        be2.skipStringList();
        be2.updateStringList(s4);
        be2.skipStringList();
        bd1 = new BinaryDecoder(record);

        s1 = bd1.readStringList();
        s2 = bd1.readStringList();
        s3 = bd1.readStringList();

        Assertions.assertEquals(s1.get(0), "a1");
        Assertions.assertEquals(s1.get(1), "b1");
        Assertions.assertEquals(s1.get(2), "c1");
        Assertions.assertEquals(s2.get(0), "a4");
        Assertions.assertEquals(s2.get(1), "b4");
        Assertions.assertEquals(s2.get(2), "c4");
        Assertions.assertEquals(s3.get(0), "a3");
        Assertions.assertEquals(s3.get(1), "b3");
        Assertions.assertEquals(s3.get(2), "c3");

    }

    // stream : 739 455 463 471 470 463 464 459 458 456
    // for : 662 436 438 441 452 443 437 438 434 435
    public void testE() {
        for (int j = 0; j < 10; j++) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 100000; i++) {
                test();
                testList();
                testStringList();
            }
            System.out.println(System.currentTimeMillis() - start);
        }
    }
}
