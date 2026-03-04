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

package io.dingodb.exec.spill;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for serializing and deserializing {@code Object[]} tuple lists to/from
 * raw byte arrays, suitable for use with {@link SpillFileManager}.
 *
 * <p>Serialization is performed using Java's built-in {@link ObjectOutputStream} which handles
 * all standard Java types (Integer, Long, Double, String, byte[], etc.) that typically appear
 * in DingoDB operator caches.
 */
public final class TupleSerializer {

    private TupleSerializer() {
    }

    /**
     * Serializes a list of tuples to a byte array.
     *
     * @param tuples list of {@code Object[]} tuples to serialize; must not be {@code null}
     * @return serialized byte array
     * @throws IOException on serialization failure
     */
    public static byte[] serialize(List<Object[]> tuples) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeInt(tuples.size());
            for (Object[] tuple : tuples) {
                oos.writeInt(tuple.length);
                for (Object obj : tuple) {
                    oos.writeObject(obj);
                }
            }
        }
        return bos.toByteArray();
    }

    /**
     * Deserializes a list of tuples from a byte array previously produced by {@link #serialize}.
     *
     * @param bytes serialized byte array
     * @return list of {@code Object[]} tuples
     * @throws IOException on deserialization failure
     */
    public static List<Object[]> deserialize(byte[] bytes) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(bis)) {
            int tupleCount = ois.readInt();
            List<Object[]> tuples = new ArrayList<>(tupleCount);
            for (int i = 0; i < tupleCount; i++) {
                int tupleLen = ois.readInt();
                Object[] tuple = new Object[tupleLen];
                for (int j = 0; j < tupleLen; j++) {
                    tuple[j] = ois.readObject();
                }
                tuples.add(tuple);
            }
            return tuples;
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize spilled tuple", e);
        }
    }
}
