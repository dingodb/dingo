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

package io.dingodb.net.netty.utils;

import io.dingodb.common.error.CommonError;
import io.dingodb.common.util.StackTraces;
import io.dingodb.net.NetError;
import io.netty.buffer.ByteBuf;
import io.protostuff.ByteBufferInput;
import io.protostuff.Input;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffOutput;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.dingodb.common.codec.PrimitiveCodec.INT_MAX_LEN;
import static io.dingodb.common.codec.PrimitiveCodec.LONG_MAX_LEN;

@Slf4j
public class Serializers {

    private static final ThreadLocal<LinkedBuffer> buffer = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(512));

    private Serializers() {
    }

    /**
     * Read int from {@code bytes}, and use VarInt load.
     */
    public static Integer readVarInt(ByteBuf buf) {
        int readerIndex = buf.readerIndex();
        int maxBytes = INT_MAX_LEN;
        int b = Byte.MAX_VALUE + 1;
        int result = 0;
        while ((maxBytes >= 0) && b > Byte.MAX_VALUE) {
            if (!buf.isReadable()) {
                buf.readerIndex(readerIndex);
                return null;
            }
            result ^= ((b = (buf.readByte() & 0XFF)) & 0X7F) << ((INT_MAX_LEN - maxBytes--) * (Byte.SIZE - 1));
        }
        return result;
    }

    /**
     * Read int from {@code bytes}, and use VarInt load.
     */
    public static Long readVarLong(ByteBuf buf) {
        int readerIndex = buf.readerIndex();
        int maxBytes = LONG_MAX_LEN;
        long b = Byte.MAX_VALUE + 1;
        long result = 0;
        while ((maxBytes >= 0) && b > Byte.MAX_VALUE) {
            if (!buf.isReadable()) {
                buf.readerIndex(readerIndex);
                return null;
            }
            result ^= ((b = (buf.readByte() & 0XFF)) & 0X7F) << ((LONG_MAX_LEN - maxBytes--) * (Byte.SIZE - 1));
        }
        return result;
    }

    public static <T> T read(byte[] bytes, Class<T> cls) {
        return read(ByteBuffer.wrap(bytes), cls);
    }

    public static <T> T read(ByteBuffer buffer, Class<T> cls) {
        ProtostuffWrapper<T> wrapper = new ProtostuffWrapper<>();
        Schema<ProtostuffWrapper> schema = RuntimeSchema.getSchema(ProtostuffWrapper.class);

        final Input input = new ByteBufferInput(buffer, true);
        try {
            schema.mergeFrom(input, wrapper);
        } catch (final IOException e) {
            CommonError.EXEC.throwFormatError("", Thread.currentThread(), e.getMessage());
        }

        return wrapper.value;
    }

    public static byte[] write(Object value) {
        Schema schema = RuntimeSchema.getSchema(ProtostuffWrapper.class);
        final ProtostuffOutput output = new ProtostuffOutput(buffer.get());
        try {
            schema.writeTo(output, new ProtostuffWrapper<>(value));
            return output.toByteArray();
        } catch (final IOException e) {
            CommonError.EXEC.throwFormatError("", Thread.currentThread(), e.getMessage());
        } finally {
            buffer.get().clear();
        }

        throw NetError.UNKNOWN.asException(StackTraces.stack());
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    static class ProtostuffWrapper<T> {
        static Schema<ProtostuffWrapper> schema = RuntimeSchema.getSchema(ProtostuffWrapper.class);

        private T value;
    }


}
