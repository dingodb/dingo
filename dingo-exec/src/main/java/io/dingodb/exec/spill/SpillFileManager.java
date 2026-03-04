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

import io.dingodb.common.log.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the lifecycle of spill files used by heavy operators to avoid OOM.
 *
 * <p>Typical usage:
 * <pre>{@code
 * SpillFileManager mgr = new SpillFileManager();
 * SpillFile sf = mgr.createSpillFile("sort-op-1");
 * mgr.write(sf, rowBatch);
 * // later …
 * try (Iterator<Object[]> it = mgr.readIterator(sf)) {
 *     while (it.hasNext()) { process(it.next()); }
 * }
 * mgr.delete(sf);
 * }</pre>
 *
 * <p>Thread safety: {@link #createSpillFile} is safe to call concurrently; all other
 * methods operate on individual {@link SpillFile} instances and must not be called
 * concurrently on the same instance.
 */
@Slf4j
public class SpillFileManager {

    private static final AtomicInteger GLOBAL_COUNTER = new AtomicInteger(0);

    private final Path spillDir;

    /**
     * Creates a manager using the directory from {@link SpillConfig}.
     */
    public SpillFileManager() {
        this(SpillConfig.getSpillDir());
    }

    /**
     * Creates a manager that writes spill files to {@code spillDirPath}.
     */
    public SpillFileManager(String spillDirPath) {
        this.spillDir = Paths.get(spillDirPath);
        try {
            Files.createDirectories(this.spillDir);
        } catch (IOException e) {
            throw new SpillException("Failed to create spill directory: " + spillDirPath, e);
        }
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Creates a new, empty spill file associated with {@code operatorId}.
     *
     * @param operatorId logical identifier of the owning operator (used for naming only)
     * @return a {@link SpillFile} ready to receive rows via {@link #write}
     */
    public SpillFile createSpillFile(String operatorId) {
        int id = GLOBAL_COUNTER.getAndIncrement();
        String filename = "spill-" + operatorId + "-" + id + ".tmp";
        Path path = spillDir.resolve(filename);
        LogUtils.debug(log, "Creating spill file: {}", path);
        return new SpillFile(operatorId, id, path);
    }

    /**
     * Appends {@code tuples} to {@code spillFile}.
     *
     * <p>This method may be called multiple times on the same {@link SpillFile}; each call
     * appends to the file in append mode so the file remains readable via
     * {@link #readIterator} afterwards.
     *
     * @param spillFile target spill file (must have been created by this manager)
     * @param tuples    rows to write; must not be {@code null}
     */
    public void write(SpillFile spillFile, List<Object[]> tuples) throws IOException {
        if (tuples == null || tuples.isEmpty()) {
            return;
        }
        // We open in append mode and use a length-prefixed object stream approach.
        // Each call writes a "segment": an int (row count) followed by that many
        // Object[] records serialised with ObjectOutputStream.
        boolean append = Files.exists(spillFile.getPath());
        try (BufferedOutputStream bos = new BufferedOutputStream(
                 Files.newOutputStream(spillFile.getPath(),
                     append
                         ? new java.nio.file.OpenOption[]{java.nio.file.StandardOpenOption.APPEND}
                         : new java.nio.file.OpenOption[]{}));
             ObjectOutputStream oos = append
                 ? new AppendableObjectOutputStream(bos)
                 : new ObjectOutputStream(bos)) {
            for (Object[] tuple : tuples) {
                oos.writeObject(tuple);
            }
            oos.flush();
        }
        spillFile.incrementRowCount(tuples.size());
        LogUtils.debug(log, "Spilled {} rows to {}", tuples.size(), spillFile.getPath());
    }

    /**
     * Returns a lazy {@link Iterator} that reads rows from {@code spillFile} one at a time.
     *
     * <p>The returned iterator holds an open file handle; callers should wrap it in a
     * try-with-resources block or call {@link SpillIterator#close()} explicitly.
     *
     * @param spillFile spill file to read
     * @return iterator of {@code Object[]} rows; never {@code null}
     */
    public SpillIterator readIterator(SpillFile spillFile) throws IOException {
        return new SpillIterator(spillFile);
    }

    /**
     * Deletes the underlying file for {@code spillFile}, ignoring any errors.
     */
    public void delete(SpillFile spillFile) {
        try {
            Files.deleteIfExists(spillFile.getPath());
            LogUtils.debug(log, "Deleted spill file: {}", spillFile.getPath());
        } catch (IOException e) {
            LogUtils.warn(log, "Failed to delete spill file {}: {}", spillFile.getPath(), e.getMessage());
        }
    }

    /**
     * Reads all rows from {@code spillFile} into memory and deletes the file.
     */
    public List<Object[]> readAndDelete(SpillFile spillFile) throws IOException {
        List<Object[]> result = new ArrayList<>((int) Math.min(spillFile.getRowCount(), Integer.MAX_VALUE));
        try (SpillIterator it = readIterator(spillFile)) {
            while (it.hasNext()) {
                result.add(it.next());
            }
        }
        delete(spillFile);
        return result;
    }

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    /**
     * Lazy iterator over a spill file.  Implements {@link AutoCloseable} so that the
     * underlying stream is released even if the caller does not exhaust the iterator.
     */
    public static final class SpillIterator implements Iterator<Object[]>, AutoCloseable {

        private final ObjectInputStream ois;
        private Object[] next;
        private boolean done;

        private SpillIterator(SpillFile spillFile) throws IOException {
            this.ois = new ObjectInputStream(
                new BufferedInputStream(Files.newInputStream(spillFile.getPath())));
            advance();
        }

        private void advance() {
            try {
                next = (Object[]) ois.readObject();
                done = false;
            } catch (java.io.EOFException e) {
                // Normal end-of-file.
                next = null;
                done = true;
                closeQuietly();
            } catch (java.io.StreamCorruptedException e) {
                // Data corruption – treat as an error.
                next = null;
                done = true;
                closeQuietly();
                throw new SpillException("Spill file data is corrupted", e);
            } catch (ClassNotFoundException | IOException e) {
                next = null;
                done = true;
                closeQuietly();
                throw new SpillException("Failed to read from spill file", e);
            }
        }

        @Override
        public boolean hasNext() {
            return !done;
        }

        @Override
        public Object[] next() {
            if (done) {
                throw new NoSuchElementException();
            }
            Object[] current = next;
            advance();
            return current;
        }

        @Override
        public void close() throws IOException {
            ois.close();
        }

        private void closeQuietly() {
            try {
                ois.close();
            } catch (IOException ignored) {
                // ignored
            }
        }
    }

    /**
     * An {@link ObjectOutputStream} that suppresses the stream header so that it can be
     * appended to an existing object stream.
     */
    private static final class AppendableObjectOutputStream extends ObjectOutputStream {
        AppendableObjectOutputStream(java.io.OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void writeStreamHeader() {
            // Do not write the stream header when appending.
        }
    }
}
