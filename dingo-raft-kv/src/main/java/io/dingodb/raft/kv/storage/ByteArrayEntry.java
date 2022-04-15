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

package io.dingodb.raft.kv.storage;

import java.util.Arrays;
import java.util.Objects;

public class ByteArrayEntry extends Entry<byte[], byte[]> {
    public ByteArrayEntry(byte[] key, byte[] value) {
        super(key, value);
    }

    public ByteArrayEntry(byte[] key) {
        super(key);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        ByteArrayEntry entry = (ByteArrayEntry) other;
        return Arrays.equals(key, entry.key) && Arrays.equals(value, entry.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(key), Arrays.hashCode(value));
    }

    @Override
    public String toString() {
        return String.format("[%s] = [%s]", Arrays.toString(getKey()), Arrays.toString(getValue()));
    }
}
