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

package io.dingodb.dingokv.serialization;

import io.dingodb.dingokv.serialization.io.InputBuf;
import io.dingodb.dingokv.serialization.io.OutputBuf;

import java.nio.ByteBuffer;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class Serializer {
    /**
     * The max buffer size for a {@link Serializer} to cached.
     */
    public static final int MAX_CACHED_BUF_SIZE = 256 * 1024;

    /**
     * The default buffer size for a {@link Serializer}.
     */
    public static final int DEFAULT_BUF_SIZE    = 512;

    public abstract <T> OutputBuf writeObject(final OutputBuf outputBuf, final T obj);

    public abstract <T> byte[] writeObject(final T obj);

    public abstract <T> T readObject(final InputBuf inputBuf, final Class<T> clazz);

    public abstract <T> T readObject(final ByteBuffer buf, final Class<T> clazz);

    public abstract <T> T readObject(final byte[] bytes, final int offset, final int length, final Class<T> clazz);

    public <T> T readObject(final byte[] bytes, final Class<T> clazz) {
        return readObject(bytes, 0, bytes.length, clazz);
    }
}
