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

import io.dingodb.common.type.DingoType;
import io.dingodb.exec.codec.AvroTupleCodec;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages per-task/per-operator spill files for external-memory processing.
 *
 * <p>Usage:
 * <ol>
 *   <li>Call {@link #write(List)} to spill tuples to the current spill file.</li>
 *   <li>Call {@link #flush()} to finalise the current spill file and start a new one.</li>
 *   <li>Call {@link #iterator()} after all writes are complete to read spilled tuples back.</li>
 *   <li>Call {@link #clean()} to delete all spill files when they are no longer needed.</li>
 * </ol>
 */
public class SpillFileManager {
    /** Default spill directory: {@code java.io.tmpdir/dingo-spill}. */
    public static final String DEFAULT_SPILL_DIR =
        System.getProperty("dingo.spill.dir",
            Paths.get(System.getProperty("java.io.tmpdir"), "dingo-spill").toString());

    private static final String FILE_PREFIX = "spill-";
    private static final String FILE_SUFFIX = ".bin";

    private final Path spillDir;
    private final AvroTupleCodec codec;
    private final String filePrefix;
    private final List<Path> spillFiles;
    private final AtomicInteger fileCounter;

    private Path currentFile;
    private OutputStream currentStream;

    /**
     * Creates a {@code SpillFileManager} using the default spill directory.
     *
     * @param taskId     identifier of the owning task, used in file names
     * @param operatorId identifier of the owning operator, used in file names
     * @param type       schema of the tuples that will be spilled
     */
    public SpillFileManager(@NonNull String taskId, @NonNull String operatorId, @NonNull DingoType type)
        throws IOException {
        this(taskId, operatorId, type, Paths.get(DEFAULT_SPILL_DIR));
    }

    /**
     * Creates a {@code SpillFileManager} using a custom spill directory.
     *
     * @param taskId     identifier of the owning task, used in file names
     * @param operatorId identifier of the owning operator, used in file names
     * @param type       schema of the tuples that will be spilled
     * @param spillDir   directory in which spill files are created
     */
    public SpillFileManager(
        @NonNull String taskId,
        @NonNull String operatorId,
        @NonNull DingoType type,
        @NonNull Path spillDir
    ) throws IOException {
        this.codec = new AvroTupleCodec(type);
        this.filePrefix = FILE_PREFIX + sanitize(taskId) + "-" + sanitize(operatorId) + "-";
        this.spillDir = spillDir;
        this.spillFiles = new ArrayList<>();
        this.fileCounter = new AtomicInteger(0);
        Files.createDirectories(spillDir);
        openNewFile();
    }

    /**
     * Writes {@code tuples} to the current open spill file.
     *
     * @param tuples list of tuples to spill; must not be {@code null}
     */
    public synchronized void write(@NonNull List<Object @NonNull []> tuples) throws IOException {
        if (tuples.isEmpty()) {
            return;
        }
        codec.encode(currentStream, tuples);
    }

    /**
     * Flushes and closes the current spill file, then opens a new one so that
     * subsequent {@link #write} calls go to a fresh file.
     */
    public synchronized void flush() throws IOException {
        closeCurrentFile();
        openNewFile();
    }

    /**
     * Returns an {@link Iterator} that streams every tuple from all spill files
     * in the order they were written.  The iterator is lazy: it opens and reads
     * files one at a time rather than loading everything into memory at once.
     *
     * <p>The caller is responsible for calling {@link #clean()} after the iterator
     * is exhausted or no longer needed.
     *
     * @return a lazy iterator over all spilled tuples
     */
    public synchronized @NonNull Iterator<Object[]> iterator() throws IOException {
        closeCurrentFile();
        return new SpillIterator(new ArrayList<>(spillFiles), codec);
    }

    /**
     * Deletes all spill files created by this manager.
     */
    public synchronized void clean() throws IOException {
        closeCurrentFile();
        for (Path file : spillFiles) {
            Files.deleteIfExists(file);
        }
        spillFiles.clear();
    }

    /** Returns the number of spill files that have been created so far. */
    public synchronized int getSpillFileCount() {
        return spillFiles.size();
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void openNewFile() throws IOException {
        String name = filePrefix + fileCounter.getAndIncrement() + FILE_SUFFIX;
        currentFile = spillDir.resolve(name);
        currentStream = new BufferedOutputStream(Files.newOutputStream(currentFile));
        spillFiles.add(currentFile);
    }

    private void closeCurrentFile() throws IOException {
        if (currentStream != null) {
            currentStream.flush();
            currentStream.close();
            currentStream = null;
        }
    }

    /** Removes characters that are not safe for use in file names. */
    private static String sanitize(@NonNull String id) {
        return id.replaceAll("[^a-zA-Z0-9_\\-]", "_");
    }

    // -------------------------------------------------------------------------
    // Inner iterator
    // -------------------------------------------------------------------------

    /**
     * Lazy iterator that reads spill files one at a time, decoding tuples incrementally.
     */
    private static final class SpillIterator implements Iterator<Object[]> {
        private final List<Path> files;
        private final AvroTupleCodec codec;
        private int fileIndex;
        private InputStream currentStream;
        private Iterator<Object[]> current;

        SpillIterator(@NonNull List<Path> files, @NonNull AvroTupleCodec codec) throws IOException {
            this.files = files;
            this.codec = codec;
            this.fileIndex = 0;
            advanceFile();
        }

        @Override
        public boolean hasNext() {
            while (current != null && !current.hasNext()) {
                try {
                    closeCurrentStream();
                    advanceFile();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to advance spill file", e);
                }
            }
            return current != null && current.hasNext();
        }

        @Override
        public Object[] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return current.next();
        }

        private void advanceFile() throws IOException {
            if (fileIndex >= files.size()) {
                current = null;
                return;
            }
            Path file = files.get(fileIndex++);
            currentStream = new BufferedInputStream(Files.newInputStream(file));
            current = codec.decodeIterator(currentStream);
        }

        private void closeCurrentStream() throws IOException {
            if (currentStream != null) {
                currentStream.close();
                currentStream = null;
            }
        }
    }
}
