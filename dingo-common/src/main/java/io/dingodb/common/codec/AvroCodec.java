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

import io.dingodb.common.table.TupleMapping;
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
import org.apache.avro.util.Utf8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;

public class AvroCodec {
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
        byte[] bytes,
        @Nonnull DatumReader<GenericRecord> reader
    ) throws IOException {
        BinaryDecoder decoder = decoderLocal.get();
        decoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(bytes), decoder);
        decoderLocal.set(decoder);
        return reader.read(null, decoder);
    }

    @Nonnull
    private static byte[] encodeBytes(
        GenericRecord record,
        @Nonnull DatumWriter<GenericRecord> writer
    ) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = encoderLocal.get();
        encoder = EncoderFactory.get().directBinaryEncoder(out, encoder);
        encoderLocal.set(encoder);
        writer.write(record, encoder);
        return out.toByteArray();
    }

    private static Object convert(Object in) {
        return in instanceof Utf8 ? in.toString() : in;
    }

    public Object[] decode(@Nonnull byte[] bytes) throws IOException {
        final GenericRecord keyRecord = decodeBytes(bytes, reader);
        int size = schema.getFields().size();
        Object[] tuple = new Object[size];
        for (int i = 0; i < size; ++i) {
            tuple[i] = convert(keyRecord.get(i));
        }
        return tuple;
    }

    public void decode(Object[] result, byte[] bytes, @Nonnull TupleMapping mapping) throws IOException {
        Object[] tuple = decode(bytes);
        mapping.map(result, tuple);
    }

    public byte[] encode(@Nonnull Object[] tuple) throws IOException {
        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < tuple.length; ++i) {
            record.put(i, tuple[i]);
        }
        return encodeBytes(record, writer);
    }

    public byte[] encode(@Nonnull Object[] tuple, @Nonnull TupleMapping mapping) throws IOException {
        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < mapping.size(); ++i) {
            record.put(i, tuple[mapping.get(i)]);
        }
        return encodeBytes(record, writer);
    }
}
