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

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages spill-to-disk lifecycle for heavy exec operators (HashJoin, Sort, Aggregate, VectorTopN).
 *
 * <p>Design goals:
 * <ul>
 *   <li>Write/read streaming of encoded tuples</li>
 *   <li>Per-file metadata (operatorId, bucketId, record/byte counts)</li>
 *   <li>Local-tmp retention — spill dir is cleaned up when all files for an operator are released</li>
 *   <li>Optional async uploader, <em>disabled by default</em></li>
 * </ul>
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * SpillFileManager mgr = SpillFileManager.getInstance();
 * SpillFile sf = mgr.createSpill("myOperator-1", 0);
 * mgr.write(sf, encodedTupleBytes);
 * mgr.closeAndFlush(sf);
 * Iterator<byte[]> it = mgr.read(sf);
 * while (it.hasNext()) {
 *     byte[] chunk = it.next();
 *     // decode and use chunk...
 * }
 * mgr.release(sf);   // deletes the backing file
 * }</pre>
 */
@Slf4j
public final class SpillFileManager {

    /** Default spill root directory. Can be overridden via {@link #setSpillDir(String)}. */
    private static final String DEFAULT_SPILL_DIR =
        System.getProperty("dingo.spill.dir",
            Paths.get(System.getProperty("java.io.tmpdir"), "dingo-spill").toString());

    /** Size of each read chunk returned by the iterator, in bytes. */
    private static final int READ_BUFFER_SIZE =
        Integer.getInteger("dingo.spill.readBufferSize", 64 * 1024);

    /** Singleton instance. */
    private static volatile SpillFileManager instance;

    /** Root directory under which all spill files are created. */
    private volatile Path spillDir;

    /**
     * Tracks all active {@link SpillFile} objects keyed by their backing file's absolute path.
     * Used for metrics and clean-up.
     */
    private final ConcurrentHashMap<String, SpillFile> activeFiles = new ConcurrentHashMap<>();

    /** Running counter of total spill files ever created. */
    private final AtomicLong totalFilesCreated = new AtomicLong(0);

    /** Running counter of total bytes ever spilled. */
    private final AtomicLong totalBytesSpilled = new AtomicLong(0);

    // ----------------------------------------------------------------------------------
    // Async uploader (disabled by default)
    // ----------------------------------------------------------------------------------

    /**
     * Flag controlling the optional async uploader.  Disabled by default; set to {@code true} via
     * {@link #setAsyncUploaderEnabled(boolean)} to activate upload-to-remote-storage behaviour.
     */
    private volatile boolean asyncUploaderEnabled = false;

    // ----------------------------------------------------------------------------------
    // Lifecycle
    // ----------------------------------------------------------------------------------

    private SpillFileManager() {
        this.spillDir = Paths.get(DEFAULT_SPILL_DIR);
    }

    /**
     * Returns the singleton {@link SpillFileManager} instance, creating it if necessary.
     */
    public static SpillFileManager getInstance() {
        if (instance == null) {
            synchronized (SpillFileManager.class) {
                if (instance == null) {
                    instance = new SpillFileManager();
                }
            }
        }
        return instance;
    }

    /**
     * Overrides the root spill directory.  Must be called before the first use of
     * {@link #createSpill} to have an effect.
     *
     * @param dir absolute path to the desired spill directory
     */
    public void setSpillDir(String dir) {
        this.spillDir = Paths.get(dir);
    }

    /**
     * Enables or disables the optional async uploader (default: disabled).
     *
     * @param enabled {@code true} to enable, {@code false} to disable
     */
    public void setAsyncUploaderEnabled(boolean enabled) {
        this.asyncUploaderEnabled = enabled;
        if (enabled) {
            log.info("SpillFileManager: async uploader ENABLED");
        } else {
            log.info("SpillFileManager: async uploader DISABLED");
        }
    }

    // ----------------------------------------------------------------------------------
    // Core API
    // ----------------------------------------------------------------------------------

    /**
     * Creates a new spill file for the given {@code operatorId} and {@code bucketId}.
     *
     * <p>The file is opened for writing immediately; call {@link #write} to append data and
     * {@link #closeAndFlush} when all data has been written.
     *
     * @param operatorId logical identifier of the calling operator (e.g. vertex id + operator type)
     * @param bucketId   partition/bucket index within the operator
     * @return a ready-to-write {@link SpillFile}
     * @throws IOException if the spill directory cannot be created or the file cannot be opened
     */
    public SpillFile createSpill(String operatorId, int bucketId) throws IOException {
        Path opDir = spillDir.resolve(sanitise(operatorId));
        Files.createDirectories(opDir);

        String fileName = bucketId + "-" + UUID.randomUUID() + ".spill";
        File file = opDir.resolve(fileName).toFile();

        SpillFile sf = new SpillFile(operatorId, bucketId, file);
        activeFiles.put(file.getAbsolutePath(), sf);
        totalFilesCreated.incrementAndGet();
        log.debug("SpillFileManager: created spill file {} for operator={} bucket={}", file, operatorId, bucketId);
        return sf;
    }

    /**
     * Appends raw bytes to the given {@link SpillFile}.
     *
     * <p>The bytes should represent one or more fully encoded tuples so that the reader can
     * decode them in the same unit. No framing is added by this method; callers are responsible
     * for encoding length-prefixed or self-delimiting records.
     *
     * @param spillFile target spill file
     * @param data      bytes to write
     * @throws IOException           on write failure
     * @throws IllegalStateException if the file has already been closed
     */
    public void write(SpillFile spillFile, byte[] data) throws IOException {
        spillFile.write(data);
        totalBytesSpilled.addAndGet(data.length);
    }

    /**
     * Flushes and closes the given {@link SpillFile}'s output stream.
     *
     * <p>After this call the file is ready for reading via {@link #read}.
     * If the async uploader is enabled, the file is queued for upload to remote storage after
     * flushing (no-op in the current implementation — hook point only).
     *
     * @param spillFile spill file to close
     * @throws IOException on flush/close failure
     */
    public void closeAndFlush(SpillFile spillFile) throws IOException {
        spillFile.closeAndFlush();
        log.debug("SpillFileManager: closed spill file {} (records={} bytes={})",
            spillFile.getFile(), spillFile.getRecordCount(), spillFile.getByteCount());
        if (asyncUploaderEnabled) {
            scheduleAsyncUpload(spillFile);
        }
    }

    /**
     * Returns an {@link Iterator} that streams the raw bytes written to the spill file.
     *
     * <p>Each {@code byte[]} returned by {@link Iterator#next()} is a chunk of at most
     * {@value #READ_BUFFER_SIZE} bytes read sequentially from the spill file.  Callers that
     * encoded fixed-width or self-delimiting records must re-parse the stream accordingly.
     *
     * @param spillFile a {@link SpillFile} that has been closed via {@link #closeAndFlush}
     * @return iterator over raw byte chunks
     * @throws IOException           if the backing file cannot be opened for reading
     * @throws IllegalStateException if the file has not yet been closed
     */
    public Iterator<byte[]> read(SpillFile spillFile) throws IOException {
        if (!spillFile.isClosed()) {
            throw new IllegalStateException(
                "SpillFile must be closed before reading: " + spillFile.getFile().getAbsolutePath());
        }
        File file = spillFile.getFile();
        if (!file.exists() || file.length() == 0) {
            return Collections.emptyIterator();
        }
        return new ChunkIterator(new BufferedInputStream(new FileInputStream(file), READ_BUFFER_SIZE));
    }

    /**
     * Reads all bytes from the given spill file into a single byte array.
     *
     * <p>This is a convenience method for callers that use a single-record-per-file serialisation
     * strategy (e.g. {@link io.dingodb.exec.spill.TupleSerializer}) and need the complete
     * content in one call.
     *
     * @param spillFile a closed {@link SpillFile}
     * @return full content of the spill file, or an empty array if the file is empty
     * @throws IOException on read failure
     */
    public byte[] readAllBytes(SpillFile spillFile) throws IOException {
        if (!spillFile.isClosed()) {
            throw new IllegalStateException(
                "SpillFile must be closed before reading: " + spillFile.getFile().getAbsolutePath());
        }
        File file = spillFile.getFile();
        if (!file.exists() || file.length() == 0) {
            return new byte[0];
        }
        return Files.readAllBytes(file.toPath());
    }

    /**
     * Releases the given {@link SpillFile} by deleting its backing file and removing it from
     * the active-file registry.
     *
     * <p>This method is idempotent; calling it multiple times on the same file is safe.
     *
     * @param spillFile the file to release
     */
    public void release(SpillFile spillFile) {
        String key = spillFile.getFile().getAbsolutePath();
        if (activeFiles.remove(key) != null) {
            if (!spillFile.delete()) {
                log.warn("SpillFileManager: failed to delete spill file {}", spillFile.getFile());
            } else {
                log.debug("SpillFileManager: released spill file {}", spillFile.getFile());
            }
            // Remove the operator sub-directory if it is now empty
            File opDir = spillFile.getFile().getParentFile();
            if (opDir != null && opDir.isDirectory()) {
                String[] remaining = opDir.list();
                if (remaining != null && remaining.length == 0) {
                    if (!opDir.delete()) {
                        log.debug("SpillFileManager: could not remove empty spill dir {}", opDir);
                    }
                }
            }
        }
    }

    /**
     * Releases all spill files associated with the given {@code operatorId}.
     *
     * @param operatorId the operator whose spill files should be deleted
     */
    public void releaseAll(String operatorId) {
        List<SpillFile> toRelease = new ArrayList<>();
        for (SpillFile sf : activeFiles.values()) {
            if (operatorId.equals(sf.getOperatorId())) {
                toRelease.add(sf);
            }
        }
        for (SpillFile sf : toRelease) {
            release(sf);
        }
    }

    // ----------------------------------------------------------------------------------
    // Metrics
    // ----------------------------------------------------------------------------------

    /** @return total number of spill files created since startup */
    public long getTotalFilesCreated() {
        return totalFilesCreated.get();
    }

    /** @return total bytes spilled since startup */
    public long getTotalBytesSpilled() {
        return totalBytesSpilled.get();
    }

    /** @return number of currently active (not yet released) spill files */
    public int getActiveFileCount() {
        return activeFiles.size();
    }

    // ----------------------------------------------------------------------------------
    // Internal helpers
    // ----------------------------------------------------------------------------------

    /**
     * Sanitises an operatorId so it can be used safely as a directory name.
     */
    private static String sanitise(String operatorId) {
        return operatorId.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    /**
     * Hook for the optional async uploader.  Currently a no-op; replace with real upload logic
     * when enabling remote storage support.
     */
    private void scheduleAsyncUpload(SpillFile spillFile) {
        // Async uploader is disabled by default.
        // Override this method or inject an UploadHandler to enable remote storage.
        log.debug("SpillFileManager: async upload scheduled for {} (no-op)", spillFile.getFile());
    }

    // ----------------------------------------------------------------------------------
    // Iterator implementation
    // ----------------------------------------------------------------------------------

    /**
     * Lazy chunk-based iterator that reads a spill file in {@value SpillFileManager#READ_BUFFER_SIZE}-byte
     * windows.  The underlying stream is closed when the file is exhausted or iteration is aborted.
     */
    private static final class ChunkIterator implements Iterator<byte[]> {

        private final InputStream is;
        private byte[] next;
        private boolean done;

        ChunkIterator(InputStream is) {
            this.is = is;
            this.done = false;
            this.next = readNext();
        }

        private byte[] readNext() {
            if (done) {
                return null;
            }
            try {
                byte[] buf = new byte[READ_BUFFER_SIZE];
                int read = is.read(buf);
                if (read == -1) {
                    done = true;
                    is.close();
                    return null;
                }
                if (read == buf.length) {
                    return buf;
                }
                // Return only the bytes actually read
                byte[] result = new byte[read];
                System.arraycopy(buf, 0, result, 0, read);
                return result;
            } catch (IOException e) {
                done = true;
                try {
                    is.close();
                } catch (IOException ignore) {
                    // ignore close error
                }
                throw new RuntimeException("Failed to read spill file", e);
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public byte[] next() {
            byte[] current = next;
            next = readNext();
            return current;
        }
    }
}
