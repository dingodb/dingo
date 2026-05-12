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

import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class SpillManager {
    public static final int DEFAULT_SPILL_THRESHOLD = 100000;

    /**
     * Shared bounded thread pool for asynchronous spill operations triggered by memory revocation.
     * Core size 0 allows threads to be reclaimed when idle; max size is capped to avoid
     * unbounded thread creation under heavy revocation pressure.
     */
    private static final ExecutorService SPILL_EXECUTOR = new ThreadPoolExecutor(
        0,
        Math.max(4, Runtime.getRuntime().availableProcessors()),
        60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        r -> {
            Thread t = new Thread(r, "dingo-spill-worker");
            t.setDaemon(true);
            return t;
        }
    );

    public static final SpillManager INSTANCE = new SpillManager();

    private final File spillDir;
    private final Set<File> activeFiles = ConcurrentHashMap.newKeySet();
    private final AtomicInteger totalCreated = new AtomicInteger(0);
    private final AtomicLong totalBytesSpilled = new AtomicLong(0);

    private SpillManager() {
        spillDir = new File(DingoConfiguration.spillDir());
        if (!spillDir.exists() && !spillDir.mkdirs()) {
            LogUtils.warn(log, "Failed to create spill directory: {}", spillDir.getAbsolutePath());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(this::cleanupAll, "dingo-spill-cleanup"));
    }

    /**
     * 在溢出目录下创建一个新的临时溢出文件。
     *
     * @param jobId      job 标识符，嵌入文件名以便追踪
     * @param operatorId operator 标识符，嵌入文件名以便追踪
     * @return the newly created {@link File}
     * @throws IOException 如果无法创建文件
     */
    public File createSpillFile(String jobId, String operatorId) throws IOException {
        File file = new File(spillDir,
            "spill-" + jobId + "-" + operatorId + "-" + UUID.randomUUID() + ".avro");
        if (!file.createNewFile()) {
            throw new IOException("Failed to create spill file: " + file.getAbsolutePath());
        }
        activeFiles.add(file);
        totalCreated.incrementAndGet();
        LogUtils.debug(log, "Created spill file: {}", file.getAbsolutePath());
        return file;
    }

    /**
     * 删除溢出文件并将其从跟踪中删除。
     *
     * @param file 要删除的文件；如果 {@code null} 或已删除则忽略`
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

    /** 返回当前打开/尚未删除的溢出文件的数量。 */
    public int getActiveFileCount() {
        return activeFiles.size();
    }

    /** 返回自 JVM 启动以来创建的溢出文件的累积数量。 */
    public int getTotalCreatedCount() {
        return totalCreated.get();
    }

    /**
     * 报告溢出文件写入的字节数，累加到全局计数器。
     *
     * @param bytes 写入的字节数
     */
    public void reportBytesSpilled(long bytes) {
        if (bytes > 0) {
            totalBytesSpilled.addAndGet(bytes);
            LogUtils.debug(log, "Reported {} bytes spilled, total={}", bytes, totalBytesSpilled.get());
        }
    }

    /** 返回自 JVM 启动以来所有溢出文件写入的累积字节数。 */
    public long getTotalBytesSpilled() {
        return totalBytesSpilled.get();
    }

    /**
     * Returns the shared thread pool for asynchronous spill I/O.
     * Use this instead of creating bare threads in operator {@code startMemoryRevoke()} methods.
     */
    public static ExecutorService getSpillExecutor() {
        return SPILL_EXECUTOR;
    }

    /** 删除所有活动的溢出文件； JVM 关闭时自动调用。 */
    public void cleanupAll() {
        for (File file : activeFiles) {
            if (file.exists()) {
                file.delete();
            }
        }
        activeFiles.clear();
    }
}
