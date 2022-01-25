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

import com.google.protobuf.ByteString;
import io.dingodb.raft.util.internal.UnsafeUtil;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class AsciiStringUtil {
    public static byte[] unsafeEncode(final CharSequence in) {
        final int len = in.length();
        final byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = (byte) in.charAt(i);
        }
        return out;
    }

    public static String unsafeDecode(final byte[] in, final int offset, final int len) {
        final char[] out = new char[len];
        for (int i = 0; i < len; i++) {
            out[i] = (char) (in[i + offset] & 0xFF);
        }
        return UnsafeUtil.moveToString(out);
    }

    public static String unsafeDecode(final byte[] in) {
        return unsafeDecode(in, 0, in.length);
    }

    public static String unsafeDecode(final ByteString in) {
        final int len = in.size();
        final char[] out = new char[len];
        for (int i = 0; i < len; i++) {
            out[i] = (char) (in.byteAt(i) & 0xFF);
        }
        return UnsafeUtil.moveToString(out);
    }

    private AsciiStringUtil() {
    }
}
