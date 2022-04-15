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

package io.dingodb.raft.kv.storage;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.zip.ScatterZipOutputStream;
import org.apache.commons.compress.archivers.zip.StreamCompressor;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.parallel.FileBasedScatterGatherBackingStore;
import org.apache.commons.compress.parallel.InputStreamSupplier;
import org.apache.commons.compress.parallel.ScatterGatherBackingStore;

import java.io.File;
import java.io.IOException;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

import static org.apache.commons.compress.archivers.zip.ZipArchiveEntryRequest.createZipArchiveEntryRequest;

@Slf4j
public class ParallelZipCreator {
    private final AtomicInteger storeNum = new AtomicInteger(0);
    private final ExecutorService executorService;
    private final Deque<Future<? extends ScatterZipOutputStream>> futures = new ConcurrentLinkedDeque<>();
    private final Deque<ScatterZipOutputStream> streams = new ConcurrentLinkedDeque<>();
    private final ThreadLocal<ScatterZipOutputStream> scatterStream = ThreadLocal.withInitial(this::scatterStream);
    private final int compressionLevel;

    public ParallelZipCreator(ExecutorService executorService, int compressionLevel) throws IllegalArgumentException {
        if (compressionLevel < -1 || compressionLevel > 9) {
            throw new IllegalArgumentException("Compression level is expected between -1~9");
        }

        this.executorService = executorService;
        this.compressionLevel = compressionLevel;
    }

    @Nonnull
    private ScatterZipOutputStream scatterStream() {
        try {
            ScatterGatherBackingStore backingStore = new FileBasedScatterGatherBackingStore(
                File.createTempFile("PZC_BS", "n" + storeNum.incrementAndGet())
            );
            ScatterZipOutputStream scatterStream = new ScatterZipOutputStream(
                backingStore, StreamCompressor.create(compressionLevel, backingStore)
            );
            streams.add(scatterStream);
            return scatterStream;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void addArchiveEntry(ZipArchiveEntry zipArchiveEntry, InputStreamSupplier source) {
        futures.add(executorService.submit(() -> {
            ScatterZipOutputStream scatterStream = this.scatterStream.get();
            scatterStream.addArchiveEntry(createZipArchiveEntryRequest(zipArchiveEntry, source));
            return scatterStream;
        }));
    }

    public void writeTo(ZipArchiveOutputStream targetStream) throws Exception {
        try {
            for (final Future<?> future : futures) {
                future.get();
            }
            for (final Future<? extends ScatterZipOutputStream> future : futures) {
                future.get().zipEntryWriter().writeNextZipEntry(targetStream);
            }
        } finally {
            closeAll();
        }
    }

    private void closeAll() {
        for (final ScatterZipOutputStream scatterStream : streams) {
            try {
                scatterStream.close();
            } catch (final IOException ex) {
                log.error("Close scatter stream error", ex);
            }
        }
    }
}

