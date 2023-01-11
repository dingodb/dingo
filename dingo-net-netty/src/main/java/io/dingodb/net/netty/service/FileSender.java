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

package io.dingodb.net.netty.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.Location;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.net.Message;
import io.dingodb.net.netty.Channel;
import io.dingodb.net.netty.NetService;
import io.dingodb.net.netty.NetServiceProvider;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static io.dingodb.net.netty.Constant.FILE_TRANSFER;
import static io.dingodb.net.netty.Constant.USER_DEFINE_T;

@Slf4j
@AutoService(io.dingodb.net.service.FileTransferService.class)
public class FileSender implements io.dingodb.net.service.FileTransferService {

    private static final NetService netService = NetServiceProvider.NET_SERVICE_INSTANCE;

    private static final int block = 1024 * 1024 * 4;

    public FileSender() {
    }

    @Override
    public void transfer(Location location, Path source, Path target) {
        log.info(String.format("FileSender::transfer Location=[%s] Path=from [%s] to [%s]",
            location.toString(), source.toString(), target.toString()));
        if (!Files.exists(source) ) {
            throw new IllegalArgumentException(source + " not found.");
        }
        if (Files.isDirectory(source)) {
            recursion(location, source, target);
            return;
        }
        Channel ch = netService.newChannel(location);
        CompletableFuture<Void> future = new CompletableFuture<>();
        ch.directListener(__ -> future.complete(null));
        ch.setCloseListener(__ -> {
            if (!future.isDone()) {
                future.completeExceptionally(new RuntimeException("Unknown!"));
            }
        });
        try (FileChannel fileChannel = FileChannel.open(source, StandardOpenOption.READ)) {
            ch.send(new Message(FILE_TRANSFER, PrimitiveCodec.encodeString(target.toAbsolutePath().toString())));
            long position = 0;
            long size = fileChannel.size();
            int read;
            ByteBuf buffer;
            while (position < size) {
                read = (int) Math.min(size - position, block);
                position += (buffer = ch.buffer(USER_DEFINE_T, read)).writeBytes(fileChannel, position, read);
                ch.send(buffer);
            }
            ch.send(ch.buffer(USER_DEFINE_T, 0));
            future.join();
        } catch (Exception e) {
            ch.close();
            throw new RuntimeException(e);
        }
    }

    private void recursion(Location location, Path source, Path target) {
        log.info(String.format("FileSender::recursion Location=[%s] Path=from [%s] to [%s]",
            location.toString(), source.toString(), target.toString()));
        File[] files = source.toFile().listFiles();
        if (files == null || files.length == 0) {
            return;
        }
        CountDownLatch countDownLatch = new CountDownLatch(files.length);
        Arrays.stream(files).map(File::toPath).forEach(_p ->
            Executors.submit(
                "transfer-to-" + location.url(),
                () -> transfer(location, _p, target.resolve(_p.getFileName()))
            ).thenRun(countDownLatch::countDown)
        );
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return;
    }

}
