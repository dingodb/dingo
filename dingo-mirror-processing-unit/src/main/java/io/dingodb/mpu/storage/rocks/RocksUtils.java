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

package io.dingodb.mpu.storage.rocks;

import io.dingodb.common.store.KeyValue;

import java.util.ArrayList;
import java.util.List;

public class RocksUtils {
    public static boolean ttlValid(int ttl) {
        return ttl > 0;
    }

    public static byte[] encodeFixed32(int value) {
        return new byte[] {(byte)(value & 0xff), (byte)((value >> 8) & 0xff), (byte)((value >> 16) & 0xff),
            (byte)((value >> 24) & 0xff)};
    }

    public static int decodeFixed32(byte[] array) {
        return (array[0] & 0xff) | (array[1] & 0xff) << 8 | (array[2] & 0xff) << 16 | (array[3] & 0xff) << 24;
    }

    public static int getCurrentTimestamp() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public static byte[] getValueWithTs(byte[] value, byte[] tsArray) {
        byte[] valueWithTs = new byte[value.length + tsArray.length];
        System.arraycopy(value, 0, valueWithTs, 0, value.length);
        System.arraycopy(tsArray, 0, valueWithTs, value.length, tsArray.length);
        return valueWithTs;
    }

    public static byte[] getValueWithTs(byte[] value, int timestamp) {
        byte[] tsArray = encodeFixed32(timestamp);
        return getValueWithTs(value, tsArray);
    }

    public static byte[] getValueWithNowTs(byte[] value) {
        return getValueWithTs(value, getCurrentTimestamp());
    }

    public static int getTsbyValue(byte[] valueWithTs) {
        byte[] tsArray = new byte[4];
        System.arraycopy(valueWithTs, valueWithTs.length - tsArray.length, tsArray, 0, tsArray.length);
        return decodeFixed32(tsArray);
    }

    public static List<KeyValue> getValueWithTsList(List<KeyValue> rows, int timestamp) {
        byte[] tsArray = encodeFixed32(timestamp);
        List<KeyValue> kvWithTsList = new ArrayList<>();
        for (final KeyValue kv : rows) {
            if (kv == null) {
                continue;
            }
            byte[] valueWithTs = getValueWithTs(kv.getValue(), tsArray);
            kvWithTsList.add(new KeyValue(kv.getKey(), valueWithTs));
        }
        return kvWithTsList;
    }

    public static List<KeyValue> getValueWithNowTsList(List<KeyValue> rows) {
        return getValueWithTsList(rows, getCurrentTimestamp());
    }
}
