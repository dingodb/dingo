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

import io.dingodb.common.codec.DingoCodec;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class DingoSerialTxRxCodec implements TxRxCodec {
    public static final int TUPLES_FLAG = 0;
    public static final int NORMAL_FIN_FLAG = 1;
    public static final int ABNORMAL_FIN_FLAG = 2;

    private final DingoType schema;
    private final DingoCodec codec;

    public DingoSerialTxRxCodec(@NonNull DingoType schema) {
        this.schema = schema;
        this.codec = new DingoCodec(schema.toDingoSchemas());
    }

    @Override
    public byte[] encodeTuples(@NonNull List<Object[]> tuples) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TUPLES_FLAG);
        for (Object[] tuple : tuples) {
            tuple = (Object[]) schema.convertTo(tuple, DingoConverter.INSTANCE);
            byte[] content = codec.encode(tuple);
            CodecUtils.encodeVarInt(bos, content.length);
            bos.write(content);
        }
        return bos.toByteArray();
    }

    @Override
    public byte[] encodeFin(Fin fin) throws IOException {
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
    public List<Object[]> decode(byte[] bytes) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        int flag = is.read();
        switch (flag) {
            case TUPLES_FLAG:
                List<Object[]> tuples = new LinkedList<>();
                while (is.available() > 0) {
                    int len = CodecUtils.decodeVarInt(is);
                    byte[] content = new byte[len];
                    is.read(content);
                    Object[] tuple = codec.decode(content);
                    tuple = (Object[]) schema.convertFrom(tuple, DingoConverter.INSTANCE);
                    tuples.add(tuple);
                }
                return tuples;
            case NORMAL_FIN_FLAG:
                return Collections.singletonList(new Object[]{FinWithProfiles.deserialize(is)});
            case ABNORMAL_FIN_FLAG:
                return Collections.singletonList(new Object[]{FinWithException.deserialize(is)});
            default:
        }
        throw new IllegalStateException("Unexpected data message flag \"" + flag + "\".");
    }
}
