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

package io.dingodb.exec.spill;

import lombok.Getter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Represents a single spill file used to persist encoded tuples to local disk.
 * Instances are created and managed exclusively through {@link SpillFileManager}.
 */
public final class SpillFile {

    /** Operator identifier that created this spill file. */
    @Getter
    private final String operatorId;

    /** Bucket/partition index within the operator. */
    @Getter
    private final int bucketId;

    /** Backing file on local disk. */
    @Getter
    private final File file;

    /** Number of records written to this spill file. */
    @Getter
    private long recordCount;

    /** Total bytes written to this spill file. */
    @Getter
    private long byteCount;

    /** Buffered output stream; non-null while file is open for writing. */
    private OutputStream outputStream;

    /** Whether this file has been closed (flushed) and is ready for reading. */
    private volatile boolean closed;

    SpillFile(String operatorId, int bucketId, File file) throws IOException {
        this.operatorId = operatorId;
        this.bucketId = bucketId;
        this.file = file;
        this.outputStream = new BufferedOutputStream(new FileOutputStream(file));
        this.closed = false;
        this.recordCount = 0L;
        this.byteCount = 0L;
    }

    /**
     * Appends raw bytes to this spill file.
     *
     * @param data bytes to write
     * @throws IOException on write failure
     * @throws IllegalStateException if this file has already been closed
     */
    void write(byte[] data) throws IOException {
        if (closed) {
            throw new IllegalStateException("SpillFile already closed: " + file.getAbsolutePath());
        }
        outputStream.write(data);
        byteCount += data.length;
        recordCount++;
    }

    /**
     * Flushes and closes the underlying output stream, making the file ready for reading.
     *
     * @throws IOException on flush/close failure
     */
    void closeAndFlush() throws IOException {
        if (!closed) {
            outputStream.flush();
            outputStream.close();
            outputStream = null;
            closed = true;
        }
    }

    /**
     * Returns {@code true} if the file has been closed and is ready for reading.
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Deletes the backing file from disk.
     *
     * @return {@code true} if the file was deleted successfully
     */
    public boolean delete() {
        return file.delete();
    }
}
