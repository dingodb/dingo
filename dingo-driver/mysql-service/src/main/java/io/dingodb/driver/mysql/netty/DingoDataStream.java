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

package io.dingodb.driver.mysql.netty;

import io.dingodb.common.log.LogUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


@Slf4j
public class DingoDataStream implements ChunkedInput<ByteBuf> {

    public final BlockingQueue<byte[]> blockingQueue = new LinkedBlockingQueue<>(1000);
    @Setter
    @Getter
    private long total;
    private long offset;

    boolean first;

    ByteBuf byteBuf;
    boolean end;

    public DingoDataStream(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
        this.total = 0;
        this.first = true;
        this.end = false;
    }

    public void addLength(long size) {
        this.total += size;
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        LogUtils.info(log, "chunk is end:{}", end);
        return end;
    }

    @Override
    public void close() throws Exception {
        this.total = 0;
        this.first = true;
        this.offset = 0;
        this.end = false;
        blockingQueue.clear();
        LogUtils.info(log, "chunk close");
    }

    @Override
    public ByteBuf readChunk(ChannelHandlerContext channelHandlerContext) throws Exception {
        LogUtils.info(log, "read chunk");
        return null;
    }

    @Override
    public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
        LogUtils.info(log, "read chunk, thread:{}", Thread.currentThread().getId());
        if (first) {
            if (byteBuf == null) {
                return null;
            }
            int readableBytes = byteBuf.readableBytes();
            byte[] bytesArray = new byte[readableBytes];
            byteBuf.getBytes(byteBuf.readerIndex(), bytesArray);
            LogUtils.info(log, "chunk size:{}", readableBytes);
            offset += readableBytes;
            ByteBuf buf =  allocator.buffer(readableBytes);
            buf.writeBytes(bytesArray);
            first = false;
            return buf;
        } else {
            byte[] bytes;
            while (true) {
                bytes = blockingQueue.poll(5000, TimeUnit.MILLISECONDS);
                if (bytes == null) {
                    continue;
                } else {
                    if (bytes.length == 0) {
                        this.end = true;
                        return null;
                    }
                    break;
                }
            }
            int bytesLength = bytes.length;
            offset += bytesLength;
            ByteBuf buf = allocator.buffer(bytesLength);
            buf.writeBytes(bytes);
            return buf;
        }
    }

    @Override
    public long length() {
        LogUtils.info(log, "chunk length:{}", total);
        return total;
    }

    @Override
    public long progress() {
        LogUtils.info(log, "chunk progress, offset:{}, block size:{}", offset, blockingQueue.size());
        return offset;
    }

}
