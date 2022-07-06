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

package io.dingodb.sdk.utils;


/**
 * Thread local buffer storage.
 */
public final class ThreadLocalData {

    /**
     * Initial buffer size on first use of thread local buffer.
     */
    public static int DefaultBufferSize = 8192;

    // 128KB
    private static final int THREAD_LOCAL_CUTOFF = 1024 * 128;

    private static final ThreadLocal<byte[]> BufferThreadLocal = new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[DefaultBufferSize];
        }
    };

    /**
     * Return thread local buffer.
     * @return byte array.
     */
    public static byte[] getBuffer() {
        return BufferThreadLocal.get();
    }

    /**
     * Resize and return thread local buffer if the requested size &lt;= 128 KB.
     * buffer will be returned from heap memory.
     * @param buffer input buffer
     * @param size new buffer size
     * @return buffer array
     */
    public static byte[] resizeBuffer(byte[] buffer, int size) {
        // Do not store extremely large buffers in thread local storage.
        if (size > THREAD_LOCAL_CUTOFF) {
            return new byte[size];
        }

        BufferThreadLocal.set(new byte[size]);
        return BufferThreadLocal.get();
    }

}
