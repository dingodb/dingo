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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.annotation.Nonnull;

public class AvroCodec implements Codec {
    private static final ThreadLocal<BinaryDecoder> decoderLocal = ThreadLocal.withInitial(() -> null);
    private static final ThreadLocal<BinaryEncoder> encoderLocal = ThreadLocal.withInitial(() -> null);

    private final Schema schema;
    private final DatumReader<GenericRecord> reader;
    private final DatumWriter<GenericRecord> writer;

    public AvroCodec(@Nonnull Schema schema) {
        this.schema = schema;
        this.reader = new GenericDatumReader<>(schema);
        this.writer = new GenericDatumWriter<>(schema);
    }

    private static GenericRecord decodeBytes(
        InputStream is,
        @Nonnull DatumReader<GenericRecord> reader
    ) throws IOException {
        BinaryDecoder decoder = decoderLocal.get();
        decoder = DecoderFactory.get().directBinaryDecoder(is, decoder);
        decoderLocal.set(decoder);
        return reader.read(null, decoder);
    }

    private static void encodeBytes(
        OutputStream os,
        GenericRecord record,
        @Nonnull DatumWriter<GenericRecord> writer
    ) throws IOException {
        BinaryEncoder encoder = encoderLocal.get();
        encoder = EncoderFactory.get().directBinaryEncoder(os, encoder);
        encoderLocal.set(encoder);
        writer.write(record, encoder);
    }

    public void encode(OutputStream os, @Nonnull Object[] tuple) throws IOException {
        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < tuple.length; ++i) {
            record.put(i, tuple[i]);
        }
        encodeBytes(os, record, writer);
    }

    public void encode(
        OutputStream os,
        @Nonnull Object[] tuple,
        @Nonnull TupleMapping mapping
    ) throws IOException {
        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < mapping.size(); ++i) {
            record.put(i, tuple[mapping.get(i)]);
        }
        encodeBytes(os, record, writer);
    }

    public byte[] encode(@Nonnull Object[] tuple) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        encode(os, tuple);
        return os.toByteArray();
    }

    public byte[] encode(@Nonnull Object[] tuple, @Nonnull TupleMapping mapping) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        encode(os, tuple, mapping);
        return os.toByteArray();
    }

    @Override
    public byte[] encode(@Nonnull byte[] origin, @Nonnull Object[] tuple, @Nonnull int[] schemaIndex)
        throws IOException, ClassCastException {
        return new byte[0];
    }

    public Object[] decode(@Nonnull InputStream is) throws IOException {
        final GenericRecord keyRecord = decodeBytes(is, reader);
        int size = schema.getFields().size();
        Object[] tuple = new Object[size];
        for (int i = 0; i < size; ++i) {
            tuple[i] = keyRecord.get(i);
        }
        return tuple;
    }

    public Object[] decode(@Nonnull byte[] bytes) throws IOException {
        return decode(new ByteArrayInputStream(bytes));
    }

    @Override
    public Object[] decode(Object[] result, byte[] bytes, @Nonnull TupleMapping mapping) throws IOException {
        Object[] tuple = decode(bytes);
        mapping.map(result, tuple);
        return result;
    }

    @Override
    public Object[] decode(byte[] bytes, @Nonnull int[] schemaIndex) throws IOException {
        return new Object[0];
    }
}
