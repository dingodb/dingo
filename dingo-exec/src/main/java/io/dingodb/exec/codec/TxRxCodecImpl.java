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

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.tuple.TupleId;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TxRxCodecImpl implements TxRxCodec {
    public static final int TUPLES_FLAG = 0;
    public static final int NORMAL_FIN_FLAG = 1;
    public static final int ABNORMAL_FIN_FLAG = 2;
    public static final int TUPLES_ID_FLAG = 3;

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
    public void encodeTupleIds(OutputStream os, List<TupleId> tupleIds) throws IOException {
        os.write(TUPLES_ID_FLAG);
        os.write(PrimitiveCodec.encodeInt(tupleIds.size()));
        List<Object[]> tuples = new ArrayList<>();
        for (TupleId tupleId : tupleIds) {
            os.write(tupleId.getPartId().encode());
            os.write(tupleId.getIndexId() == null ? CommonId.EMPTY_INDEX.encode(): tupleId.getIndexId().encode());
            tuples.add(tupleId.getTuple());
        }
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
    public List<TupleId> decode(byte[] bytes) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        int flag = is.read();
        switch (flag) {
            case TUPLES_FLAG:
                return codec.decode(is).stream().map(t -> TupleId.builder().tuple(t).build()).collect(Collectors.toList());
            case NORMAL_FIN_FLAG:
                return Collections.singletonList(TupleId.builder().tuple(new Object[]{FinWithProfiles.deserialize(is)}).build());
            case ABNORMAL_FIN_FLAG:
                return Collections.singletonList(TupleId.builder().tuple(new Object[]{FinWithException.deserialize(is)}).build());
            case TUPLES_ID_FLAG:
                byte[] sizeByte = new byte[4];
                is.read(sizeByte, 0, 4);
                int size = PrimitiveCodec.decodeInt(sizeByte);
                List<CommonId> partIds = new ArrayList<>();
                List<CommonId> indexIds = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    byte[] b1 = new byte[CommonId.LEN];
                    is.read(b1, 0, CommonId.LEN);
                    partIds.add(CommonId.decode(b1));
                    byte[] b2 = new byte[CommonId.LEN];
                    is.read(b2, 0 ,CommonId.LEN);
                    indexIds.add(CommonId.decode(b2));
                }
                List<Object[]> tuples = codec.decode(is);
                List<TupleId> tupleIds = new ArrayList<>();
                for (int i = 0; i < partIds.size(); i++) {
                    CommonId indexId = indexIds.get(i);
                    tupleIds.add(TupleId.builder()
                        .partId(partIds.get(i))
                        .tuple(tuples.get(i))
                        .indexId(indexId.equals(CommonId.EMPTY_INDEX) ? null : indexId)
                        .build()
                    );
                }
                return tupleIds;
            default:
        }
        throw new IllegalStateException("Unexpected data message flag \"" + flag + "\".");
    }
}
