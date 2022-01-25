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

package io.dingodb.raft.util;

import java.util.Arrays;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class Bytes implements Comparable<Bytes> {
    private static final char[] HEX_CHARS_UPPER = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    private final byte[] bytes;

    // cache the hash code for the string, default to 0
    private int hashCode;

    public static Bytes wrap(byte[] bytes) {
        return new Bytes(bytes);
    }

    /**
     * Create a Bytes using the byte array.
     *
     * @param bytes This array becomes the backing storage for the object.
     */
    public Bytes(byte[] bytes) {
        this.bytes = bytes;

        // initialize hash code to 0
        hashCode = 0;
    }

    /**
     * Get the data from the Bytes.
     * @return The underlying byte array
     */
    public byte[] get() {
        return this.bytes;
    }

    /**
     * The hashcode is cached except for the case where it is computed as 0, in which
     * case we compute the hashcode on every call.
     *
     * @return the hashcode
     */
    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Arrays.hashCode(bytes);
        }

        return hashCode;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }

        // we intentionally use the function to compute hashcode here
        return this.hashCode() == other.hashCode() && other instanceof Bytes
               && Arrays.equals(this.bytes, ((Bytes) other).get());
    }

    @Override
    public int compareTo(Bytes that) {
        return BytesUtil.getDefaultByteArrayComparator().compare(this.bytes, that.bytes);
    }

    @Override
    public String toString() {
        return Bytes.toString(bytes, 0, bytes.length);
    }

    /**
     * Write a printable representation of a byte array. Non-printable
     * characters are hex escaped in the format \\x%02X, eg:
     * \x00 \x05 etc.
     *
     * This function is brought from org.apache.hadoop.hbase.util.Bytes
     *
     * @param b array to write out
     * @param off offset to start at
     * @param len length to write
     * @return string output
     */
    @SuppressWarnings("SameParameterValue")
    private static String toString(final byte[] b, int off, int len) {
        final StringBuilder result = new StringBuilder();

        if (b == null) {
            return result.toString();
        }

        // just in case we are passed a 'len' that is > buffer length...
        if (off >= b.length) {
            return result.toString();
        }

        if (off + len > b.length) {
            len = b.length - off;
        }

        for (int i = off; i < off + len; ++i) {
            final int ch = b[i] & 0xFF;
            if (ch >= ' ' && ch <= '~' && ch != '\\') {
                result.append((char) ch);
            } else {
                result.append("\\x");
                result.append(HEX_CHARS_UPPER[ch / 0x10]);
                result.append(HEX_CHARS_UPPER[ch % 0x10]);
            }
        }
        return result.toString();
    }
}
