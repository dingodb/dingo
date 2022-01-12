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

package io.dingodb.store.row.serialization.impl.protostuff.io;

import com.alipay.sofa.jraft.util.internal.UnsafeUtil;
import io.dingodb.store.row.serialization.io.InputBuf;
import io.protostuff.ByteArrayInput;
import io.protostuff.Input;
import io.protostuff.ProtobufException;

import java.nio.ByteBuffer;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class Inputs {
    public static Input getInput(final InputBuf inputBuf) {
        if (UnsafeUtil.hasUnsafe() && inputBuf.hasMemoryAddress()) {
            return new UnsafeNioBufInput(inputBuf.nioByteBuffer(), true);
        }
        return new NioBufInput(inputBuf.nioByteBuffer(), true);
    }

    public static Input getInput(final ByteBuffer buf) {
        if (UnsafeUtil.hasUnsafe() && buf.isDirect()) {
            return new UnsafeNioBufInput(buf, true);
        }
        return new NioBufInput(buf, true);
    }

    public static Input getInput(final byte[] bytes, final int offset, final int length) {
        return new ByteArrayInput(bytes, offset, length, true);
    }

    public static void checkLastTagWas(final Input input, final int value) throws ProtobufException {
        if (input instanceof UnsafeNioBufInput) {
            ((UnsafeNioBufInput) input).checkLastTagWas(value);
        } else if (input instanceof NioBufInput) {
            ((NioBufInput) input).checkLastTagWas(value);
        } else if (input instanceof ByteArrayInput) {
            ((ByteArrayInput) input).checkLastTagWas(value);
        }
    }

    private Inputs() {
    }
}
