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

package io.dingodb.server.executor.service;

import com.google.auto.service.AutoService;
import io.dingodb.codec.CodecServiceProvider;
import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import lombok.AllArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static io.dingodb.server.executor.common.Mapping.mapping;

public final class CodecService implements io.dingodb.codec.CodecService {

    public static final CodecService INSTANCE = new CodecService();

    @AutoService(CodecServiceProvider.class)
    public static final class Provider implements CodecServiceProvider {
        @Override
        public CodecService get() {
            return INSTANCE;
        }
    }

    private CodecService() {
    }

    //
    // Codec service.
    //

    @AllArgsConstructor
    static class KeyValueCodec implements io.dingodb.codec.KeyValueCodec {
        private final DingoKeyValueCodec delegate;

        @Override
        public Object[] decode(KeyValue keyValue) throws IOException {
            return delegate.decode(mapping(keyValue));
        }

        @Override
        public Object[] decodeKey(byte @NonNull [] bytes) throws IOException {
            throw new UnsupportedEncodingException();
        }

        @Override
        public KeyValue encode(Object @NonNull [] tuple) throws IOException {
            return mapping(delegate.encode(tuple));
        }

        @Override
        public byte[] encodeKey(Object[] keys) throws IOException {
            return delegate.encodeKey(keys);
        }

        @Override
        public Object[] mapKeyAndDecodeValue(Object[] keys, byte[] bytes) throws IOException {
            throw new UnsupportedEncodingException();
        }

        @Override
        public byte[] encodeKeyPrefix(Object[] record, int columnCount) throws IOException {
            return delegate.encodeKeyPrefix(record, columnCount);
        }
    }

    @Override
    public KeyValueCodec createKeyValueCodec(CommonId id, DingoType type, TupleMapping keyMapping) {
        return new KeyValueCodec(new DingoKeyValueCodec(mapping(type), mapping(keyMapping), id.seq));
    }

}
