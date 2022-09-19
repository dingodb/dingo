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

import io.dingodb.common.type.TupleMapping;
import io.dingodb.serial.io.RecordDecoder;
import io.dingodb.serial.io.RecordEncoder;
import io.dingodb.serial.schema.DingoSchema;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;

public class DingoCodec implements Codec {
    private RecordEncoder re;
    private RecordDecoder rd;
    TupleMapping mapping;

    public DingoCodec(List<DingoSchema> schemas) {
        this(schemas, null, false);
    }

    public DingoCodec(List<DingoSchema> schemas, TupleMapping mapping) {
        this(schemas, mapping, false);
    }

    public DingoCodec(List<DingoSchema> schemas, TupleMapping mapping, boolean isKey) {
        this.re = new RecordEncoder(schemas, (short) 0, isKey);
        this.rd = new RecordDecoder(schemas, (short) 0, isKey);
        this.mapping = mapping;
    }

    @Override
    public byte[] encode(@Nonnull Object[] tuple) throws IOException, ClassCastException {
        return re.encode(tuple);
    }

    @Override
    public byte[] encodeKey(@Nonnull Object[] tuple) throws IOException, ClassCastException {
        return re.encodeKey(tuple);
    }

    @Override
    public byte[] encodeKeyForRangeScan(@Nonnull Object[] tuple) throws IOException, ClassCastException {
        return re.encodeKeyWithoutLength(tuple);
    }

    @Override
    public byte[] encode(@Nonnull Object[] tuple, @Nonnull TupleMapping mapping)
        throws IOException, ClassCastException {
        Object[] newTuple = new Object[mapping.size()];
        int i = 0;
        for (int index : mapping.getMappings()) {
            newTuple[index] = tuple[i++];
        }
        return re.encode(newTuple);
    }

    @Override
    public byte[] encodeKey(@Nonnull Object[] tuple, @Nonnull TupleMapping mapping) throws IOException, ClassCastException {
        Object[] newTuple = new Object[mapping.size()];
        int i = 0;
        for (int index : mapping.getMappings()) {
            newTuple[index] = tuple[i++];
        }
        return re.encodeKey(newTuple);
    }

    @Override
    public byte[] encode(@Nonnull byte[] origin, @Nonnull Object[] tuple, @Nonnull int[] schemaIndex)
        throws IOException, ClassCastException {
        return re.encode(origin, schemaIndex, tuple);
    }

    @Override
    public byte[] encodeKey(@Nonnull byte[] origin, @Nonnull Object[] tuple, @Nonnull int[] schemaIndex) throws IOException, ClassCastException {
        return re.encodeKey(origin, schemaIndex, tuple);
    }

    @Override
    public Object[] decode(@Nonnull byte[] bytes) throws IOException {
        return rd.decode(bytes);
    }

    @Override
    public Object[] decodeKey(@Nonnull byte[] bytes) throws IOException {
        return rd.decodeKey(bytes);
    }

    @Override
    public Object[] decode(Object[] result, byte[] bytes, @Nonnull TupleMapping mapping) throws IOException {
        Object[] tuple = decode(bytes);
        mapping.map(result, tuple);
        return result;
    }

    @Override
    public Object[] decodeKey(Object[] result, byte[] bytes, @Nonnull TupleMapping mapping) throws IOException {
        Object[] tuple = decodeKey(bytes);
        mapping.map(result, tuple);
        return result;
    }

    @Override
    public Object[] decode(byte[] bytes, @Nonnull int[] schemaIndex) throws IOException {
        return rd.decode(bytes, schemaIndex);
    }

    @Override
    public Object[] decodeKey(byte[] bytes, @Nonnull int[] schemaIndex) throws IOException {
        return rd.decodeKey(bytes, schemaIndex);
    }
}
