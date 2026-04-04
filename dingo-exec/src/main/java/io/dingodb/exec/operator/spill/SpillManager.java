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
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Singleton manager for operator spill-to-disk resources.
 *
 * <p>Handles creation and lifecycle management of temporary spill files used when
 * operator intermediate state (sort buffers, hash-join build sides, etc.) exceeds the
 * configured in-memory threshold.
 *
 * <p>Configuration via system properties:
 * <ul>
 *   <li>{@code dingo.spill.dir}  – directory for spill files (default: {@code <tmpdir>/dingo-spill})</li>
 *   <li>{@code dingo.spill.threshold} – number of tuples to buffer in memory before spilling
 *       (default: 100 000)</li>
 * </ul>
 */
@Slf4j
public final class SpillManager {

    public static final SpillManager INSTANCE = new SpillManager();

    /**
     * Default spill directory; overridable via {@code -Ddingo.spill.dir=<path>}.
     */
    private static final String SPILL_DIR_PATH = System.getProperty(
        "dingo.spill.dir",
        System.getProperty("java.io.tmpdir") + File.separator + "dingo-spill"
    );

    /**
     * Default in-memory tuple threshold before spilling;
     * overridable via {@code -Ddingo.spill.threshold=<n>}.
     */
    public static final int DEFAULT_SPILL_THRESHOLD = Integer.parseInt(
        System.getProperty("dingo.spill.threshold", "100000")
    );

    private final File spillDir;
    private final Set<File> activeFiles = ConcurrentHashMap.newKeySet();
    private final AtomicInteger totalCreated = new AtomicInteger(0);

    private SpillManager() {
        spillDir = new File(SPILL_DIR_PATH);
        if (!spillDir.exists() && !spillDir.mkdirs()) {
            LogUtils.warn(log, "Failed to create spill directory: {}", spillDir.getAbsolutePath());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(this::cleanupAll, "dingo-spill-cleanup"));
    }

    /**
     * Creates a new temporary spill file under the spill directory.
     *
     * @return the newly created {@link File}
     * @throws IOException if the file cannot be created
     */
    public File createSpillFile() throws IOException {
        File file = new File(spillDir, "spill-" + UUID.randomUUID() + ".avro");
        activeFiles.add(file);
        totalCreated.incrementAndGet();
        LogUtils.debug(log, "Created spill file: {}", file.getAbsolutePath());
        return file;
    }

    /**
     * Deletes a spill file and removes it from tracking.
     *
     * @param file the file to delete; ignored if {@code null} or already deleted
     */
    public void deleteSpillFile(File file) {
        if (file == null) {
            return;
        }
        activeFiles.remove(file);
        if (file.exists() && !file.delete()) {
            LogUtils.warn(log, "Failed to delete spill file: {}", file.getAbsolutePath());
        } else {
            LogUtils.debug(log, "Deleted spill file: {}", file.getAbsolutePath());
        }
    }

    /** Returns the number of spill files currently open / not yet deleted. */
    public int getActiveFileCount() {
        return activeFiles.size();
    }

    /** Returns the cumulative number of spill files created since JVM start. */
    public int getTotalCreatedCount() {
        return totalCreated.get();
    }

    /** Deletes all active spill files; called automatically on JVM shutdown. */
    public void cleanupAll() {
        for (File file : activeFiles) {
            if (file.exists()) {
                file.delete();
            }
        }
        activeFiles.clear();
    }
}
