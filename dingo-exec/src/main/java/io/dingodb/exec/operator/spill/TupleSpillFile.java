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

package io.dingodb.exec.operator.spill;

import io.dingodb.common.log.LogUtils;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.codec.AvroTupleCodec;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A single spill file that stores serialized tuples on disk using the Avro binary codec.
 *
 * <p>Usage pattern:
 * <ol>
 *   <li>Write tuples in batches via {@link #write(List)}, then call {@link #finishWrite()}.</li>
 *   <li>Iterate over stored tuples lazily via {@link #iterator()}.</li>
 *   <li>Call {@link #close()} to release resources and delete the underlying file.</li>
 * </ol>
 *
 * <p>Instances are not thread-safe; callers must synchronize externally if needed.
 */
@Slf4j
public class TupleSpillFile implements Closeable {

    private static final int IO_BUFFER_SIZE = 64 * 1024; // 64 KB

    @Getter
    private final File file;
    private final AvroTupleCodec codec;

    private OutputStream outputStream;
    private boolean writing;

    @Getter
    private long tupleCount;

    /**
     * Creates a new spill file backed by the given {@code file}, using {@code schema} for encoding.
     *
     * @param file   the backing file (created if it does not exist; must be writable)
     * @param schema the DingoType describing the tuple layout
     * @throws IOException if the output stream cannot be opened
     */
    public TupleSpillFile(File file, DingoType schema) throws IOException {
        this.file = file;
        this.codec = new AvroTupleCodec(schema);
        this.outputStream = new BufferedOutputStream(new FileOutputStream(file), IO_BUFFER_SIZE);
        this.writing = true;
        this.tupleCount = 0;
    }

    /**
     * Appends a batch of tuples to the spill file.
     *
     * @param tuples the tuples to write; must not be {@code null}
     * @throws IOException           if a write error occurs
     * @throws IllegalStateException if called after {@link #finishWrite()}
     */
    public void write(List<Object[]> tuples) throws IOException {
        if (!writing) {
            throw new IllegalStateException("Spill file is not in write mode");
        }
        codec.encode(outputStream, tuples);
        tupleCount += tuples.size();
    }

    /**
     * Flushes and closes the write stream, preparing the file for reading.
     *
     * <p>This method is idempotent; calling it more than once is safe.
     *
     * @throws IOException if closing the stream fails
     */
    public void finishWrite() throws IOException {
        if (writing && outputStream != null) {
            outputStream.flush();
            outputStream.close();
            outputStream = null;
            writing = false;
            LogUtils.debug(log, "Spill file {} finished write, tupleCount={}", file.getName(), tupleCount);
        }
    }

    /**
     * Returns a lazy, forward-only iterator over the tuples stored in this file.
     *
     * <p>Calls {@link #finishWrite()} automatically if the file is still in write mode.
     *
     * @return an iterator yielding stored tuples in insertion order
     * @throws IOException if the file cannot be opened for reading
     */
    public Iterator<Object[]> iterator() throws IOException {
        if (writing) {
            finishWrite();
        }
        return new LazyTupleIterator(new BufferedInputStream(new FileInputStream(file), IO_BUFFER_SIZE));
    }

    /**
     * Closes the file and deletes the underlying temporary file.
     *
     * <p>Safe to call multiple times.
     */
    @Override
    public void close() {
        try {
            finishWrite();
        } catch (IOException ignored) {
            // Best effort
        }
        SpillManager.INSTANCE.deleteSpillFile(file);
    }

    // -------------------------------------------------------------------------
    // Lazy tuple iterator
    // -------------------------------------------------------------------------

    /**
     * A forward-only iterator that decodes tuples from an {@link InputStream} one at a time.
     * The stream is closed when the last record has been read.
     */
    private final class LazyTupleIterator implements Iterator<Object[]> {

        private final InputStream inputStream;
        private Object[] lookahead;
        private boolean done;

        LazyTupleIterator(InputStream inputStream) throws IOException {
            this.inputStream = inputStream;
            this.done = false;
            this.lookahead = fetchNext();
        }

        @Override
        public boolean hasNext() {
            return lookahead != null;
        }

        @Override
        public Object[] next() {
            if (lookahead == null) {
                throw new NoSuchElementException("No more tuples in spill file");
            }
            Object[] result = lookahead;
            try {
                lookahead = fetchNext();
            } catch (IOException e) {
                throw new RuntimeException("Error reading spill file " + file.getAbsolutePath(), e);
            }
            return result;
        }

        private Object[] fetchNext() throws IOException {
            if (done) {
                return null;
            }
            Object[] tuple;
            try {
                tuple = codec.decodeOne(inputStream);
            } catch (IOException e) {
                // Clean up on read failure then re-throw
                closeStream();
                throw e;
            }
            if (tuple == null) {
                closeStream();
                return null;
            }
            return tuple;
        }

        private void closeStream() {
            if (!done) {
                done = true;
                try {
                    inputStream.close();
                } catch (IOException ignored) {
                    // Best effort
                }
            }
        }
    }
}
