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

import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.util.Utils;
import io.dingodb.raft.util.CRC64;
import io.dingodb.raft.util.Requires;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Future;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;

import static io.dingodb.common.util.NoBreakFunctionWrapper.throwException;
import static io.dingodb.common.util.NoBreakFunctionWrapper.wrap;

public class ParallelZipCompressor {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelZipCompressor.class);

    private ParallelZipCompressor() {
    }

    public static Checksum compress(File source, File zipFile) throws Exception {
        if (source == null) {
            return null;
        }
        Checksum checksum = new CRC64();
        Files.createDirectories(Paths.get(zipFile.getParent()));
        ParallelZipCreator zipCreator = new ParallelZipCreator(Deflater.DEFAULT_COMPRESSION);

        compress(source, zipCreator, ZipEntry.DEFLATED);

        try (
            FileOutputStream fos = new FileOutputStream(zipFile);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            CheckedOutputStream cos = new CheckedOutputStream(bos, checksum);
            ZipArchiveOutputStream archiveOutputStream = new ZipArchiveOutputStream(cos)
        ) {
            zipCreator.writeTo(archiveOutputStream);
            archiveOutputStream.flush();
            fos.getFD().sync();
        }
        return checksum;
    }

    private static void compress(File source, ParallelZipCreator zipCreator, int method) {
        if (source == null) {
            return;
        }
        if (source.isFile()) {
            addEntry(source, zipCreator, ZipEntry.DEFLATED);
            return;
        }
        File[] files = Requires.requireNonNull(source.listFiles(), "files");
        for (File file : files) {
            String child = Paths.get(source.getPath(), file.getName()).toString();
            if (file.isDirectory()) {
                compress(file, zipCreator, method);
            } else {
                addEntry(new File(child), zipCreator, method);
            }
        }
    }

    private static void addEntry(File file, ParallelZipCreator zipCreator, int method) {
        ZipArchiveEntry archiveEntry = new ZipArchiveEntry(file, file.getName());
        archiveEntry.setMethod(method);
        zipCreator.addArchiveEntry(archiveEntry, () -> {
            try {
                return file.isDirectory() ? new NullInputStream(0) : new BufferedInputStream(new FileInputStream(file));
            } catch (FileNotFoundException e) {
                LOG.error("Compress entry error, Can't find file, path={}.", file.getPath());
            }
            return new NullInputStream(0) ;
        });
    }

    public static void deCompress(String source, String out, String checksum) throws Exception {
        if (!Long.toHexString(computeZipFileChecksumValue(source).getValue()).equals(checksum)) {
            throw new RuntimeException("Checksum failed.");
        }

        try (ZipFile zipFile = new ZipFile(source)) {
            List<Future<Boolean>> futures = new ArrayList<>();
            for (Enumeration<ZipArchiveEntry> e = zipFile.getEntries(); e.hasMoreElements(); ) {
                ZipArchiveEntry zipEntry = e.nextElement();
                Future<Boolean> future = Executors.submit("zip-decompress", () -> deCompress(zipFile, zipEntry, out));
                futures.add(future);
            }
            for (Future<Boolean> future : futures) {
                future.get();
            }
        }

    }

    private static boolean deCompress(ZipFile zipFile, ZipArchiveEntry entry, String out) throws Exception {
        File targetFile = new File(Paths.get(out, entry.getName()).toString());
        FileUtils.forceMkdir(targetFile.getParentFile());
        try (
            InputStream is = zipFile.getInputStream(entry);
            BufferedInputStream fis = new BufferedInputStream(is);
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(targetFile))
        ) {
            IOUtils.copy(fis, bos);
        }
        return true;
    }

    private static Checksum computeZipFileChecksumValue(String zipPath) throws Exception {
        Checksum checksum = new CRC64();
        try (
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(zipPath));
            CheckedInputStream cis = new CheckedInputStream(bis, checksum);
            ZipArchiveInputStream zis = new ZipArchiveInputStream(cis);
        ) {
            Utils.emptyWhile(wrap(() -> zis.getNextZipEntry() != null, throwException()));
        }
        return checksum;
    }

}
