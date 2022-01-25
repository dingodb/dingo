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

package io.dingodb.store.row.serialization.io;

import io.dingodb.raft.util.internal.ReferenceFieldUpdater;
import io.dingodb.raft.util.internal.Updaters;
import io.dingodb.store.row.serialization.Serializer;

import java.io.ByteArrayOutputStream;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class OutputStreams {
    private static final ReferenceFieldUpdater<ByteArrayOutputStream, byte[]> bufUpdater
        = Updaters.newReferenceFieldUpdater(ByteArrayOutputStream.class, "buf");

    // Reuse the byte[] in ByteArrayOutputStream
    private static final ThreadLocal<ByteArrayOutputStream>                   bufThreadLocal
        = ThreadLocal.withInitial(() -> new ByteArrayOutputStream(Serializer.DEFAULT_BUF_SIZE));

    public static ByteArrayOutputStream getByteArrayOutputStream() {
        return bufThreadLocal.get();
    }

    public static void resetBuf(final ByteArrayOutputStream buf) {
        buf.reset(); // for reuse

        // prevent large blocks of memory from being held strong reference
        if (bufUpdater.get(buf).length > Serializer.MAX_CACHED_BUF_SIZE) {
            bufUpdater.set(buf, new byte[Serializer.DEFAULT_BUF_SIZE]);
        }
    }

    private OutputStreams() {
    }
}
