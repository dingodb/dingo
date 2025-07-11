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

package io.dingodb.common.parser;

import org.apache.commons.lang.ArrayUtils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public final class ByteString {
    public static final ByteString EMPTY = new ByteString(new byte[0], 0, 0, Charset.defaultCharset());

    private final byte[] value;
    private final int offset;
    private final int length;
    private Charset charset;

    private transient String generateStr;
    private transient boolean multiLine = true;

    public ByteString(byte[] value, int offset, int length, Charset charset) {
        assert offset >= 0 && offset <= value.length;
        assert offset + length <= value.length;
        this.value = value;
        this.offset = offset;
        this.length = length;
        this.charset = charset;
    }

    public ByteString(byte[] value, Charset charset) {
        this(value, 0, value.length, charset);
    }

    public ByteString(byte[] value, String charsetStr) {
        try {
            this.charset = Charset.forName(charsetStr);
        } catch (Exception e) {
            this.charset = Charset.defaultCharset();
        }

        this.value = value;
        this.offset = 0;
        this.length = value.length;
    }

    public static ByteString from(String str) {
        Charset defaultCharset = Charset.defaultCharset();
        return new ByteString(str.getBytes(defaultCharset), defaultCharset);
    }

    public Charset getCharset() {
        return charset;
    }

    public void getBytes(int startPos, int endPos, byte[] dest, int destBegin) {
        if (endPos > length || startPos < 0) {
            throw new IndexOutOfBoundsException();
        } else if (startPos > endPos) {
            throw new IllegalArgumentException();
        }
        System.arraycopy(value, offset + startPos, dest, destBegin, endPos - startPos);
    }

    public void getBytes(int startPos, int endPos, ByteBuffer buffer) {
        if (endPos > length || startPos < 0) {
            throw new IndexOutOfBoundsException();
        } else if (startPos > endPos) {
            throw new IllegalArgumentException();
        }
        buffer.put(value, offset + startPos, endPos - startPos);
    }

    public byte[] getBytes(int start, int end) {
        byte[] bytes = new byte[end - start];
        getBytes(start, end, bytes, 0);
        return bytes;
    }

    public byte[] getBytes() {
        return getBytes(0, length);
    }

    public int indexOf(String target, int fromIndex) {
        if (fromIndex > length) {
            throw new IndexOutOfBoundsException();
        }
        byte[] targetBytes = target.getBytes(charset);
        return indexOf(value, offset, length, targetBytes, 0, targetBytes.length, fromIndex);
    }

    public int indexOf(String target) {
        return indexOf(target, 0);
    }

    public int indexOf(char target, int fromIndex) {
        if (fromIndex > length) {
            throw new IndexOutOfBoundsException();
        }
        int index = ArrayUtils.indexOf(value, (byte) target, offset + fromIndex);
        if (index != -1) {
            return index - offset;
        } else {
            return -1;
        }
    }

    public int indexOf(char target) {
        return indexOf(target, 0);
    }

    /**
     * Adapted from `java.lang.String#indexOf(char[], int, int, char[], int, int, int)`
     */
    private static int indexOf(byte[] source, int sourceOffset, int sourceCount,
                               byte[] target, int targetOffset, int targetCount,
                               int fromIndex) {
        if (fromIndex >= sourceCount) {
            return (targetCount == 0 ? sourceCount : -1);
        }
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        if (targetCount == 0) {
            return fromIndex;
        }

        byte first = target[targetOffset];
        int max = sourceOffset + (sourceCount - targetCount);

        for (int i = sourceOffset + fromIndex; i <= max; i++) {
            /* Look for first character. */
            if (source[i] != first) {
                while (++i <= max && source[i] != first)
                    ;
            }

            /* Found first character, now look at the rest of v2 */
            if (i <= max) {
                int j = i + 1;
                int end = j + targetCount - 1;
                for (int k = targetOffset + 1; j < end && source[j]
                    == target[k]; j++, k++)
                    ;

                if (j == end) {
                    /* Found whole string. */
                    return i - sourceOffset;
                }
            }
        }
        return -1;
    }

    /**
     * Adapted from `java.lang.String#startsWith(String, int)`
     */
    public boolean startsWith(String prefix, int toffset) {
        toffset += offset;
        byte[] ta = value;
        int to = toffset;
        byte[] pa = prefix.getBytes(charset);
        int po = 0;
        int pc = pa.length;
        // Note: toffset might be near -1>>>1.
        if ((toffset < 0) || (toffset > ta.length - pc)) {
            return false;
        }
        while (--pc >= 0) {
            if (ta[to++] != pa[po++]) {
                return false;
            }
        }
        return true;
    }

    public boolean startsWith(String prefix) {
        return startsWith(prefix, 0);
    }

    public String substring(int startPos, int endPos) {
        return new String(value, offset + startPos, endPos - startPos, charset);
    }

    public String substring(int startPos) {
        return substring(startPos, length);
    }

    public int length() {
        return length;
    }

    public char charAt(int i) {
        return (char) (value[offset + i] & 0xff);
    }

    @Override
    public String toString() {
        if (generateStr == null) {
            generateStr = new String(value, offset, length, charset);
        }
        return generateStr;
    }

    public boolean regionMatches(boolean ignoreCase, int toffset,
                                 String other, int ooffset, int len) {
        toffset += offset;
        byte ta[] = value;
        int to = toffset;
        byte pa[] = other.getBytes(charset);
        int po = ooffset;
        // Note: toffset, ooffset, or len might be near -1>>>1.
        if ((ooffset < 0) || (toffset < 0)
            || (toffset > (long) value.length - len)
            || (ooffset > (long) pa.length - len)) {
            return false;
        }
        while (len-- > 0) {
            byte c1 = ta[to++];
            byte c2 = pa[po++];
            if (c1 == c2) {
                continue;
            }
            if (ignoreCase) {
                // If characters don't match but case may be ignored,
                // try converting both characters to uppercase.
                // If the results match, then the comparison scan should
                // continue.
                byte u1 = (byte) Character.toUpperCase(c1);
                byte u2 = (byte) Character.toUpperCase(c2);
                if (u1 == u2) {
                    continue;
                }
                // Unfortunately, conversion to uppercase does not work properly
                // for the Georgian alphabet, which has strange rules about case
                // conversion.  So we need to make one last check before
                // exiting.
                if (Character.toLowerCase(u1) == Character.toLowerCase(u2)) {
                    continue;
                }
            }
            return false;
        }
        return true;
    }

    public boolean isEmpty() {
        return length == 0;
    }

    /**
     * Similar to substring but returns a ByteString
     */
    public ByteString slice(int startPos, int endPos) {
        if (endPos > length || startPos < 0) {
            throw new IndexOutOfBoundsException();
        } else if (startPos > endPos) {
            throw new IllegalArgumentException();
        }
        return new ByteString(value, offset + startPos, endPos - startPos, charset);
    }

    public ByteString slice(int startPos) {
        return slice(startPos, this.length);
    }

    public boolean isMultiLine() {
        return multiLine;
    }

    public void setMultiLine(boolean multiLine) {
        this.multiLine = multiLine;
    }
}
