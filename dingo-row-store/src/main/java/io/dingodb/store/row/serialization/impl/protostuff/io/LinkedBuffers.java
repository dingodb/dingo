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

import io.dingodb.store.row.serialization.Serializer;
import io.protostuff.LinkedBuffer;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class LinkedBuffers {
    // reuse the 'byte[]' of LinkedBuffer's head node
    private static final ThreadLocal<LinkedBuffer> bufThreadLocal
        = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(Serializer.DEFAULT_BUF_SIZE));

    public static LinkedBuffer getLinkedBuffer() {
        return bufThreadLocal.get();
    }

    public static void resetBuf(final LinkedBuffer buf) {
        buf.clear();
    }

    private LinkedBuffers() {
    }
}
