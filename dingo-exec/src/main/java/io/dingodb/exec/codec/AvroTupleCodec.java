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

package io.dingodb.exec.codec;

import io.dingodb.common.type.DingoType;
import io.dingodb.exec.tuple.TupleId;
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
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class AvroTupleCodec implements TupleCodec {
    private static final ThreadLocal<BinaryDecoder> decoderLocal = ThreadLocal.withInitial(() -> null);
    private static final ThreadLocal<BinaryEncoder> encoderLocal = ThreadLocal.withInitial(() -> null);

    private final DingoType type;
    private final Schema schema;
    private final DatumReader<GenericRecord> reader;
    private final DatumWriter<GenericRecord> writer;

    public AvroTupleCodec(@NonNull DingoType type) {
        this.type = type;
        this.schema = AvroSchemaConverter.INSTANCE.visit(type);
        this.reader = new GenericDatumReader<>(schema);
        this.writer = new GenericDatumWriter<>(schema);
    }

    private static void encodeBytes(
        OutputStream os,
        GenericRecord record,
        @NonNull DatumWriter<GenericRecord> writer
    ) throws IOException {
        BinaryEncoder encoder = encoderLocal.get();
        encoder = EncoderFactory.get().directBinaryEncoder(os, encoder);
        encoderLocal.set(encoder);
        writer.write(record, encoder);
    }

    private static @Nullable GenericRecord decodeBytes(
        InputStream is,
        GenericRecord record,
        @NonNull DatumReader<GenericRecord> reader
    ) throws IOException {
        BinaryDecoder decoder = decoderLocal.get();
        decoder = DecoderFactory.get().directBinaryDecoder(is, decoder);
        decoderLocal.set(decoder);
        try {
            return reader.read(record, decoder);
        } catch (EOFException e) {
            return null;
        }
    }

    public void encode(@NonNull OutputStream os, @NonNull List<Object @NonNull []> tuples) throws IOException {
        GenericRecord record = new GenericData.Record(schema);
        for (Object[] tuple : tuples) {
            tuple = (Object[]) type.convertTo(tuple, AvroDataConverter.INSTANCE);
            for (int i = 0; i < tuple.length; ++i) {
                record.put(i, tuple[i]);
            }
            encodeBytes(os, record, writer);
        }
    }

    public @NonNull List<Object @NonNull []> decode(@NonNull InputStream is) throws IOException {
        List<Object[]> tuples = new LinkedList<>();
        GenericRecord record = decodeBytes(is, null, reader);
        while (record != null) {
            int size = schema.getFields().size();
            Object[] tuple = new Object[size];
            for (int i = 0; i < size; ++i) {
                tuple[i] = record.get(i);
            }
            tuple = (Object[]) type.convertFrom(tuple, AvroDataConverter.INSTANCE);
            tuples.add(tuple);
            record = decodeBytes(is, record, reader);
        }
        return tuples;
    }

    /**
     * Returns an {@link Iterator} that decodes tuples one at a time from {@code is}.
     *
     * <p>The supplied {@code InputStream} is read incrementally; it is the caller's
     * responsibility to close it after the iterator is exhausted or no longer needed.
     *
     * @param is the stream to decode tuples from
     * @return a lazy iterator over the decoded tuples
     */
    public @NonNull Iterator<Object[]> decodeIterator(@NonNull InputStream is) {
        return new Iterator<Object[]>() {
            private GenericRecord next = fetchNext(null);

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Object[] next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                int size = schema.getFields().size();
                Object[] tuple = new Object[size];
                for (int i = 0; i < size; ++i) {
                    tuple[i] = next.get(i);
                }
                tuple = (Object[]) type.convertFrom(tuple, AvroDataConverter.INSTANCE);
                next = fetchNext(next);
                return tuple;
            }

            private GenericRecord fetchNext(GenericRecord reuse) {
                try {
                    return decodeBytes(is, reuse, reader);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to decode tuple from spill stream", e);
                }
            }
        };
    }
}
