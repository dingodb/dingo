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
import io.dingodb.net.netty.NettyNetService;
import io.dingodb.net.netty.NettyNetServiceProvider;
import io.dingodb.net.netty.channel.Channel;
import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.net.Message.FILE_TRANSFER;
import static io.dingodb.net.netty.packet.Type.USER_DEFINE;

@AutoService(io.dingodb.net.service.FileTransferService.class)
public class FileTransferService implements io.dingodb.net.service.FileTransferService {

    private static final NettyNetService netService = NettyNetServiceProvider.NET_SERVICE_INSTANCE;

    static {
        netService.registerTagMessageListener(
            FILE_TRANSFER,
            (msg, ch) -> {
                try {
                    ((Channel) ch).directListener(
                        new Receiver(Paths.get(PrimitiveCodec.readString(msg.content())), (Channel) ch)
                    );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
    }

    private final int block = 1024 * 1024 * 4;

    public FileTransferService() {
    }

    @Override
    public void mkdir(Location location, Path target) {
        // todo
    }

    @Override
    public void transfer(Location location, Path source, Path target) {
        if (!Files.exists(source) ) {
            throw new IllegalArgumentException(source + " not found.");
        }
        if (Files.isDirectory(source)) {
            File[] files = source.toFile().listFiles();
            if (files == null || files.length == 0) {
                return;
            }
            CountDownLatch countDownLatch = new CountDownLatch(files.length);
            Arrays.stream(files).map(File::toPath).forEach(_p ->
                Executors.submit(
                    "transfer-to-" + location.getUrl(),
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
            long position = 0, size = fileChannel.size();
            int read;
            ByteBuf buffer;
            while (position < size) {
                read = (int) Math.min(size - position, block);
                position += (buffer = ch.buffer(USER_DEFINE, read)).writeBytes(fileChannel, position, read);
                ch.send(buffer);
            }
            ch.send(ch.buffer(USER_DEFINE, 0));
            future.join();
        } catch (Exception e) {
            ch.close();
            throw new RuntimeException(e);
        }
    }

    static class Receiver implements Consumer<ByteBuffer> {

        private final FileChannel fileChannel;
        private final Path path;
        private final Channel channel;

        public Receiver(Path path, Channel channel) throws Exception {
            Files.deleteIfExists(path);
            Files.createDirectories(path.getParent());
            this.path = path;
            this.fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            this.channel = channel;
            channel.setCloseListener(wrap(ch -> {
                fileChannel.close();
            }));
        }

        @Override
        public void accept(ByteBuffer buffer) {
            try {
                if (!buffer.hasRemaining()) {
                    channel.send(Message.EMPTY);
                    channel.close();
                } else {
                    this.fileChannel.write(buffer);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

}
