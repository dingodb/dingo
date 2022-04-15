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

package io.dingodb.common.codec;

import io.dingodb.common.CommonId;
import io.dingodb.common.error.CommonError;
import io.dingodb.common.util.StackTraces;
import io.protostuff.ByteBufferInput;
import io.protostuff.Input;
import io.protostuff.LinkedBuffer;
import io.protostuff.Output;
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

public class ProtostuffCodec {

    private static final ThreadLocal<LinkedBuffer> buffer = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(1024));

    private ProtostuffCodec() {
    }

    public static <T> T read(byte[] bytes) {
        return read(ByteBuffer.wrap(bytes), null);
    }

    public static <T> T read(byte[] bytes, T source) {
        return read(ByteBuffer.wrap(bytes), source);
    }

    public static <T> T read(ByteBuffer buffer) {
        return read(buffer, null);
    }

    public static <T> T read(ByteBuffer buffer, T source) {
        ProtostuffWrapper<T> wrapper = new ProtostuffWrapper<>(source);
        Schema<ProtostuffWrapper> schema = RuntimeSchema.getSchema(ProtostuffWrapper.class);

        final Input input = new ByteBufferInput(buffer, true);
        try {
            schema.mergeFrom(input, wrapper);
        } catch (final IOException e) {
            CommonError.EXEC.throwFormatError("protostuff read", Thread.currentThread(), e.getMessage());
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
            CommonError.EXEC.throwFormatError("protostuff write", Thread.currentThread(), e.getMessage());
        } finally {
            buffer.get().clear();
        }

        throw CommonError.UNKNOWN.asException(StackTraces.stack());
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
