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

package io.dingodb.net.netty;

import io.dingodb.net.BufferOutputStream;
import io.netty.buffer.ByteBuf;
import org.checkerframework.checker.nullness.qual.NonNull;

public class NettyBufferOutputStream extends BufferOutputStream {
    private final ByteBuf buffer;

    public NettyBufferOutputStream(@NonNull Connection connection, int size) {
        buffer = connection.alloc().buffer(size);
    }

    @Override
    public void write(int b) {
        buffer.writeByte(b);
    }

    @Override
    public void write(byte @NonNull [] bytes) {
        buffer.writeBytes(bytes);
    }

    @Override
    public void write(byte @NonNull [] bytes, int off, int len) {
        buffer.writeBytes(bytes, off, len);
    }

    @Override
    public int bytes() {
        return buffer.readableBytes();
    }

    @Override
    public ByteBuf getBuffer() {
        return buffer;
    }
}
