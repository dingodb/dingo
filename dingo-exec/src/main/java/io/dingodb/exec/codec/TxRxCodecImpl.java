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
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public class TxRxCodecImpl implements TxRxCodec {
    public static final int TUPLES_FLAG = 0;
    public static final int NORMAL_FIN_FLAG = 1;
    public static final int ABNORMAL_FIN_FLAG = 2;

    private final TupleCodec codec;

    public TxRxCodecImpl(@NonNull DingoType schema) {
        this.codec = new AvroTupleCodec(schema);
    }

    @Override
    public void encodeTuples(@NonNull OutputStream os, @NonNull List<Object[]> tuples) throws IOException {
        os.write(TUPLES_FLAG);
        codec.encode(os, tuples);
    }

    @Override
    public void encodeFin(OutputStream os, Fin fin) throws IOException {
        if (fin instanceof FinWithProfiles) {
            os.write(NORMAL_FIN_FLAG);
        } else {
            os.write(ABNORMAL_FIN_FLAG);
        }
        fin.writeStream(os);
    }

    @Override
    public List<Object[]> decode(byte[] bytes) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        int flag = is.read();
        switch (flag) {
            case TUPLES_FLAG:
                return codec.decode(is);
            case NORMAL_FIN_FLAG:
                return Collections.singletonList(new Object[]{FinWithProfiles.deserialize(is)});
            case ABNORMAL_FIN_FLAG:
                return Collections.singletonList(new Object[]{FinWithException.deserialize(is)});
            default:
        }
        throw new IllegalStateException("Unexpected data message flag \"" + flag + "\".");
    }
}
