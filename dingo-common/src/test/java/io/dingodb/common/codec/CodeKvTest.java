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

package io.dingodb.common.codec;

import io.dingodb.common.util.Pair;
import io.jsonwebtoken.lang.Assert;
import org.junit.jupiter.api.Test;

public class CodeKvTest {
    public static void main(String[] args) {
    }

    @Test
    public void test3() {
        byte[] t = new byte[3];
        t[0] = 1;
        t[1] = 2;
        t[2] = 3;
        encodeAndDecode(t);
    }

    @Test
    public void test8() {
        byte[] t = new byte[8];
        for (int i = 0; i < t.length; i ++) {
            t[i] = (byte) i;
        }
        encodeAndDecode(t);
    }

    public static void encodeAndDecode(byte[] origin) {
        byte[] res = CodecKvUtil.encodeBytes(origin);
        for (byte b : res) {
            System.out.print(" " + b);
        }

        System.out.println("");

        Pair<byte[],byte[]> pair = CodecKvUtil.decodeBytes(res);
        byte[] t1 = pair.getValue();
        boolean compare = compare(t1, origin);
        byte[] key = pair.getKey();
        Assert.isTrue(compare && key.length == 0);
    }

    @Test
    public void test9() {
        byte[] t = new byte[9];
        for (int i = 0; i < t.length; i ++) {
            t[i] = (byte) i;
        }
        encodeAndDecode(t);
    }

    @Test
    public void test16() {
        byte[] t = new byte[16];
        for (int i = 0; i < t.length; i ++) {
            t[i] = (byte) i;
        }
        encodeAndDecode(t);
    }

    @Test
    public void test18() {
        byte[] t = new byte[18];
        for (int i = 0; i < t.length; i ++) {
            t[i] = (byte) i;
        }
        encodeAndDecode(t);
    }

    @Test
    public void test24() {
        byte[] t = new byte[24];
        for (int i = 0; i < t.length; i ++) {
            t[i] = (byte) i;
        }
        encodeAndDecode(t);
    }

    @Test
    public void test25() {
        byte[] t = new byte[25];
        for (int i = 0; i < t.length; i ++) {
            t[i] = (byte) i;
        }
        encodeAndDecode(t);
    }

    public static boolean compare(byte[] t1, byte[] t) {
        if (t1.length == t.length) {
            boolean same = true;
            for (int i =0; i < t.length; i ++) {
                if (t1[i] != t[i]) {
                    same = false;
                    break;
                }
            }
            return same;
        }
        return false;
    }

    @Test
    public void encodeHashDataKey(){
        String key = "DBS";
        String field = "DB:2";
        byte[] keyBytes = key.getBytes();
        byte[] fieldBytes = field.getBytes();
        byte[] kvKey = CodecKvUtil.encodeHashDataKey(key.getBytes(), field.getBytes());
        System.out.println(kvKey.length);

        Pair<byte[], byte[]> pair = CodecKvUtil.decodeHashDataKey(kvKey);
        byte[] tmpKeyBytes = pair.getKey();
        byte[] tmpFieldBytes = pair.getValue();
        boolean com1 = compare(keyBytes, tmpKeyBytes);
        boolean com2 = compare(fieldBytes, tmpFieldBytes);
        Assert.isTrue(com1 && com2);
    }

    @Test
    public void hashDataKeyPrefix() {
        String key = "DBS";
        String field = "DB:2";
        byte[] keyBytes = key.getBytes();
        byte[] fieldBytes = field.getBytes();
        byte[] dataPrefix = CodecKvUtil.hashDataKeyPrefix(keyBytes);
        byte[] end = CodecKvUtil.hashDataKeyPrefixUpperBound(dataPrefix);
        System.out.println("---");


        byte[] kvKey = CodecKvUtil.encodeHashDataKey(key.getBytes(), field.getBytes());
        System.out.println("------+++");
    }
}
