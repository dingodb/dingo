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

import java.io.InputStream;
import java.nio.ByteBuffer;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface InputBuf {
    /**
     * Exposes this backing data's readable bytes as an {@link InputStream}.
     */
    InputStream inputStream();

    /**
     * Exposes this backing data's readable bytes as a NIO {@link ByteBuffer}.
     */
    ByteBuffer nioByteBuffer();

    /**
     * Returns the number of readable bytes.
     */
    int size();

    /**
     * Returns {@code true} if and only if this buf has a reference to the low-level memory address that points
     * to the backing data.
     */
    boolean hasMemoryAddress();

    /**
     * Decreases the reference count by {@code 1} and deallocates this object if the reference count reaches at
     * {@code 0}.
     */
    boolean release();
}
