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

package io.dingodb.common.table;

import io.dingodb.common.codec.AvroCodec;

import java.io.IOException;
import javax.annotation.Nonnull;

public class KeyRecordCodec {
    private final AvroCodec keyCodec;
    private final AvroCodec recordCodec;
    private final TupleMapping keyMapping;

    public KeyRecordCodec(@Nonnull TupleSchema schema, @Nonnull TupleMapping keyMapping) {
        this.keyMapping = keyMapping;
        keyCodec = new AvroCodec(schema.select(keyMapping).getAvroSchema());
        recordCodec = new AvroCodec(schema.getAvroSchema());
    }

    public byte[] encode(@Nonnull Object[] record) throws IOException {
        return recordCodec.encode(record);
    }

    public byte[] encodeKey(@Nonnull Object[] keys) throws IOException {
        return keyCodec.encode(keys);
    }

    public byte[] encodeKeyByRecord(@Nonnull Object[] record) throws IOException {
        return keyCodec.encode(record, keyMapping);
    }
}
