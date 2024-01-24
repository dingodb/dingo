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

import io.dingodb.common.codec.protostuff.DateSchema;
import io.dingodb.common.codec.protostuff.DingoSqlExceptionSchema;
import io.dingodb.common.codec.protostuff.TimeSchema;
import io.dingodb.common.codec.protostuff.TimestampSchema;
import io.dingodb.common.error.CommonError;
import io.dingodb.common.exception.DingoSqlException;
import io.dingodb.common.util.StackTraces;
import io.protostuff.ByteArrayInput;
import io.protostuff.ByteBufferInput;
import io.protostuff.CodedInput;
import io.protostuff.GraphByteArrayInput;
import io.protostuff.GraphCodedInput;
import io.protostuff.GraphProtostuffOutput;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Slf4j
public final class ProtostuffCodec {
    private static final ProtostuffCodec INSTANCE = new ProtostuffCodec();

    private static final ThreadLocal<LinkedBuffer> buffer = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(1024));

    private final Schema<ProtostuffWrapper> schema;

    private ProtostuffCodec() {
        System.setProperty("protostuff.runtime.preserve_null_elements", "true");
        System.setProperty("protostuff.runtime.morph_collection_interfaces", "true");
        System.setProperty("protostuff.runtime.morph_map_interfaces", "true");
        System.setProperty("protostuff.runtime.morph_non_final_pojos", "true");
        RuntimeSchema.register(Date.class, DateSchema.INSTANCE);
        RuntimeSchema.register(Time.class, TimeSchema.INSTANCE);
        RuntimeSchema.register(Timestamp.class, TimestampSchema.INSTANCE);
        RuntimeSchema.register(DingoSqlException.class, DingoSqlExceptionSchema.INSTANCE);
        schema = RuntimeSchema.getSchema(ProtostuffWrapper.class);
    }

    public static <T> void registerSchema(Class<T> cls, Schema<T> schema) {
        RuntimeSchema.register(cls, schema);
    }

    public static <T> T read(byte[] bytes) {
        return INSTANCE.readMessage(bytes, 0, null);
    }

    public static <T> T read(byte[] bytes, T source) {
        return INSTANCE.readMessage(bytes, 0, source);
    }

    public static <T> T read(ByteBuffer buffer) {
        return INSTANCE.readMessage(buffer.array(), buffer.position(), null);
    }

    public static <T> T read(ByteBuffer buffer, T source) {
        return INSTANCE.readMessage(buffer.array(), buffer.position(), source);
    }

    public static byte[] write(Object value) {
        return INSTANCE.writeMessage(value);
    }

    @SuppressWarnings("unchecked")
    private <T> T readMessage(byte[] content, int pos, T source) {
        ProtostuffWrapper wrapper = new ProtostuffWrapper(source);
        GraphCodedInput input = new GraphCodedInput(new CodedInput(content, pos, content.length - pos, true));
        try {
            schema.mergeFrom(input, wrapper);
        } catch (final IOException e) {
            log.error("Protosutff read error, thread: {}.", Thread.currentThread(), e);
            CommonError.EXEC.throwFormatError("protostuff read", Thread.currentThread(), e.getMessage());
        }
        return (T) wrapper.value;
    }

    private byte[] writeMessage(Object value) {
        final ProtostuffOutput protostuffOutput = new ProtostuffOutput(buffer.get());
        GraphProtostuffOutput graphProtostuffOutput = new GraphProtostuffOutput(protostuffOutput);
        try {
            schema.writeTo(graphProtostuffOutput, new ProtostuffWrapper(value));
            return protostuffOutput.toByteArray();
        } catch (final IOException e) {
            log.error("Protosutff write error, thread: {}.", Thread.currentThread(), e);
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
    static class ProtostuffWrapper {
        private Object value;
    }
}
