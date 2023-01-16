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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BinaryTest {

    @Test
    public void test() {
        BinaryEncoder be = new BinaryEncoder(new byte[80]);

        be.writeBoolean(false); //length 2
        be.writeBoolean(null);
        be.writeKeyShort((short) 1); //length 3
        be.writeKeyShort(null);
        be.writeKeyInt(1); //length 5
        be.writeKeyInt(null);
        be.writeKeyLong(1L); //length 9
        be.writeKeyLong(null);
        be.writeKeyFloat(1f); //length 5
        be.writeKeyFloat(null);
        be.writeKeyDouble(1d); //length 9
        be.writeKeyDouble(null);
        be.writeString("123");
        be.writeString("测试长度超过80的情况");
        be.writeString("");
        be.writeString(null);

        byte[] result = be.getByteArray();

        BinaryDecoder bd = new BinaryDecoder(result);
        assertEquals(bd.readBoolean(), false);
        assertEquals(bd.readBoolean(), null);
        assertEquals(bd.readKeyShort(), (short) 1);
        assertEquals(bd.readKeyShort(), null);
        assertEquals(bd.readKeyInt(), 1);
        assertEquals(bd.readKeyInt(), null);
        assertEquals(bd.readKeyLong(), 1L);
        assertEquals(bd.readKeyLong(), null);
        assertEquals(bd.readKeyFloat(), 1f);
        assertEquals(bd.readKeyFloat(), null);
        assertEquals(bd.readKeyDouble(), 1d);
        assertEquals(bd.readKeyDouble(), null);
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
    public void testKeyBytes() {
        BinaryEncoder be1 = new BinaryEncoder(100, 12);
        byte[] bytes1 = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        byte[] bytes2 = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        byte[] bytes3 = new byte[] {};
        be1.writeKeyBytes(bytes1);
        be1.writeKeyBytes(bytes2);
        be1.writeKeyBytes(bytes3);
        byte[] bytes = be1.getByteArray();
        BinaryDecoder bd = new BinaryDecoder(bytes);
        byte[] bytes11 = bd.readKeyBytes();
        byte[] bytes22 = bd.readKeyBytes();
        byte[] bytes33 = bd.readKeyBytes();
        Arrays.equals(bytes1, bytes11);
        Arrays.equals(bytes2, bytes22);
        Arrays.equals(bytes3, bytes33);

        BinaryEncoder be2 = new BinaryEncoder(100, 12);
        be2.writeKeyBytes(bytes3);
        be2.writeKeyBytes(bytes2);
        be2.writeKeyBytes(bytes1);
        bytes = be2.getByteArray();
        bd = new BinaryDecoder(bytes);
        bytes11 = bd.readKeyBytes();
        bytes22 = bd.readKeyBytes();
        bytes33 = bd.readKeyBytes();
        Arrays.equals(bytes3, bytes11);
        Arrays.equals(bytes2, bytes22);
        Arrays.equals(bytes1, bytes33);

        BinaryEncoder be3 = new BinaryEncoder(100, 12);
        be3.writeKeyBytes(bytes2);
        be3.writeKeyBytes(bytes3);
        be3.writeKeyBytes(bytes1);
        bytes = be3.getByteArray();
        bd = new BinaryDecoder(bytes);
        bytes11 = bd.readKeyBytes();
        bytes22 = bd.readKeyBytes();
        bytes33 = bd.readKeyBytes();
        Arrays.equals(bytes2, bytes11);
        Arrays.equals(bytes3, bytes22);
        Arrays.equals(bytes1, bytes33);

        BinaryEncoder be4 = new BinaryEncoder(bytes, 12);
        be4.skipKeyBytes();
        be4.updateKeyBytes(bytes1);
        be4.skipKeyBytes();
        bytes = be4.getByteArray();
        bd = new BinaryDecoder(bytes);
        bytes11 = bd.readKeyBytes();
        bytes22 = bd.readKeyBytes();
        bytes33 = bd.readKeyBytes();
        Arrays.equals(bytes2, bytes11);
        Arrays.equals(bytes1, bytes22);
        Arrays.equals(bytes1, bytes33);
    }

    @Test
    public void testString() {
        BinaryEncoder be1 = new BinaryEncoder(100, 12);
        String str1 = "Hello World";
        String str2 = "Hello World 2";
        String str3 = "";
        be1.writeKeyString(str1);
        be1.writeKeyString(str2);
        be1.writeKeyString(str3);
        byte[] bytes = be1.getByteArray();
        BinaryDecoder bd = new BinaryDecoder(bytes);
        String str11 = bd.readKeyString();
        String str22 = bd.readKeyString();
        String str33 = bd.readKeyString();
        assertEquals(str1, str11);
        assertEquals(str2, str22);
        assertEquals(str3, str33);

        BinaryEncoder be2 = new BinaryEncoder(100, 12);
        be2.writeKeyString(str3);
        be2.writeKeyString(str2);
        be2.writeKeyString(str1);
        bytes = be2.getByteArray();
        bd = new BinaryDecoder(bytes);
        str11 = bd.readKeyString();
        str22 = bd.readKeyString();
        str33 = bd.readKeyString();
        assertEquals(str3, str11);
        assertEquals(str2, str22);
        assertEquals(str1, str33);

        BinaryEncoder be3 = new BinaryEncoder(100, 12);
        be3.writeKeyString(str2);
        be3.writeKeyString(str3);
        be3.writeKeyString(str1);
        bytes = be3.getByteArray();
        bd = new BinaryDecoder(bytes);
        str11 = bd.readKeyString();
        str22 = bd.readKeyString();
        str33 = bd.readKeyString();
        assertEquals(str2, str11);
        assertEquals(str3, str22);
        assertEquals(str1, str33);

        BinaryEncoder be4 = new BinaryEncoder(bytes, 12);
        be4.skipKeyString();
        be4.updateKeyString(str1);
        be4.skipKeyString();
        bytes = be4.getByteArray();
        bd = new BinaryDecoder(bytes);
        str11 = bd.readKeyString();
        str22 = bd.readKeyString();
        str33 = bd.readKeyString();
        assertEquals(str2, str11);
        assertEquals(str1, str22);
        assertEquals(str1, str33);
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

    @Test
    public void testBytesList() {
        BinaryEncoder be = new BinaryEncoder(1);
        List<byte[]> b1 = new ArrayList<>();
        b1.add(new byte[] {1, 2, 3});
        b1.add(new byte[] {4, 5, 6});
        b1.add(new byte[] {7, 8, 9});
        List<byte[]> b2 = new ArrayList<>();
        b2.add(new byte[] {11, 12, 13});
        b2.add(new byte[] {14, 15, 16});
        b2.add(new byte[] {17, 18, 19});
        List<byte[]> b3 = new ArrayList<>();
        b3.add(new byte[] {21, 22, 23});
        b3.add(new byte[] {24, 25, 26});
        b3.add(new byte[] {27, 28, 29});

        be.writeBytesList(b1);
        be.writeBytesList(b2);
        be.writeBytesList(b3);

        byte[] record = be.getByteArray();

        BinaryDecoder bd = new BinaryDecoder(record);
        List<byte[]> b11 = bd.readBytesList();
        List<byte[]> b22 = bd.readBytesList();
        List<byte[]> b33 = bd.readBytesList();

        for (int i = 0; i < 3; i++) {
            Assertions.assertArrayEquals(b1.get(i), b11.get(i));
            Assertions.assertArrayEquals(b2.get(i), b22.get(i));
            Assertions.assertArrayEquals(b3.get(i), b33.get(i));
        }

        b2.add(new byte[] {1, 1});

        BinaryEncoder be2 = new BinaryEncoder(record);
        be2.skipStringList();
        be2.updateBytesList(b2);
        be2.skipStringList();
        record = be2.getByteArray();
        bd = new BinaryDecoder(record);
        b11 = bd.readBytesList();
        b22 = bd.readBytesList();
        b33 = bd.readBytesList();

        for (int i = 0; i < 3; i++) {
            Assertions.assertArrayEquals(b1.get(i), b11.get(i));
            Assertions.assertArrayEquals(b2.get(i), b22.get(i));
            Assertions.assertArrayEquals(b3.get(i), b33.get(i));
        }
        Assertions.assertArrayEquals(b2.get(3), new byte[] {1, 1});
    }

    @Test
    public void testKeyDouble() {
        BinaryEncoder be = new BinaryEncoder(1);
        be.writeKeyDouble(-1.1);
        be.writeKeyDouble(-2.2);
        be.writeKeyDouble(0.0);
        be.writeKeyDouble(3.3);
        byte[] record = be.getByteArray();
        BinaryDecoder bd = new BinaryDecoder(record);
        double d1 = bd.readKeyDouble();
        double d2 = bd.readKeyDouble();
        double d3 = bd.readKeyDouble();
        double d4 = bd.readKeyDouble();
        assertEquals(d1, -1.1);
        assertEquals(d2, -2.2);
        assertEquals(d3, 0.0);
        assertEquals(d4, 3.3);
    }

    @Test
    public void testKeyFloat() {
        BinaryEncoder be = new BinaryEncoder(1);
        be.writeKeyFloat(-1.1f);
        be.writeKeyFloat(-2.2f);
        be.writeKeyFloat(0.0f);
        be.writeKeyFloat(3.3f);
        byte[] record = be.getByteArray();
        BinaryDecoder bd = new BinaryDecoder(record);
        float f1 = bd.readKeyFloat();
        float f2 = bd.readKeyFloat();
        float f3 = bd.readKeyFloat();
        float f4 = bd.readKeyFloat();
        assertEquals(f1, -1.1f);
        assertEquals(f2, -2.2f);
        assertEquals(f3, 0.0f);
        assertEquals(f4, 3.3f);
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
