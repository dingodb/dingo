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

import io.dingodb.common.codec.AvroCodec;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;

public final class AvroTxRxCodec implements TxRxCodec {
    public static final int TUPLE_FLAG = 0;
    public static final int NORMAL_FIN_FLAG = 1;
    public static final int ABNORMAL_FIN_FLAG = 2;

    private final AvroCodec avroCodec;

    public AvroTxRxCodec(@Nonnull TupleSchema schema) {
        this.avroCodec = new AvroCodec(schema.getAvroSchema());
    }

    @Nonnull
    @Override
    public byte[] encode(Object[] tuple) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        os.write(TUPLE_FLAG);
        avroCodec.encode(os, tuple);
        return os.toByteArray();
    }

    @Nonnull
    public byte[] encodeFin(@Nonnull Fin fin) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        if (fin instanceof FinWithProfiles) {
            os.write(NORMAL_FIN_FLAG);
        } else {
            os.write(ABNORMAL_FIN_FLAG);
        }
        fin.writeStream(os);
        return os.toByteArray();
    }

    @Override
    public Object[] decode(byte[] bytes) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        int flag = is.read();
        switch (flag) {
            case TUPLE_FLAG:
                return avroCodec.decode(is);
            case NORMAL_FIN_FLAG:
                return new Object[]{FinWithProfiles.deserialize(is)};
            case ABNORMAL_FIN_FLAG:
                return new Object[]{FinWithException.deserialize(is)};
            default:
        }
        throw new IllegalStateException("Unexpected data message flag \"" + flag + "\".");
    }
}
