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

package io.dingodb.raft.storage.io;

import io.dingodb.raft.error.RetryAgainException;
import io.dingodb.raft.util.ByteBufferCollector;

import java.io.IOException;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface FileReader {
    int EOF = -1;

    /**
     * Get the file path.
     *
     * @return path of the file
     */
    String getPath();

    /**
     * Read file into buf starts from offset at most maxCount.
     *
     * @param buf      read bytes into this buf
     * @param fileName file name
     * @param offset   the offset of file
     * @param maxCount max read bytes
     * @return -1 if reaches end, else return read count.
     * @throws IOException if some I/O error occurs
     * @throws RetryAgainException if it's not allowed to read partly
     * or it's allowed but throughput is throttled to 0, try again.
     */
    int readFile(final ByteBufferCollector buf, final String fileName, final long offset, final long maxCount)
                                                                                                              throws IOException,
                                                                                                              RetryAgainException;
}
