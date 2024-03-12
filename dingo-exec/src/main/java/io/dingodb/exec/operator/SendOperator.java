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

package io.dingodb.exec.operator;

import io.dingodb.exec.channel.SendEndpoint;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.SendParam;
import io.dingodb.exec.tuple.TupleId;
import io.dingodb.net.BufferOutputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public final class SendOperator extends SinkOperator {
    public static final SendOperator INSTANCE = new SendOperator();
    public static final int SEND_BATCH_SIZE = 256;

    private SendOperator() {

    }

    @Override
    public boolean push(Context context, Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            try {
                SendParam param = vertex.getParam();
                TupleId.TupleIdBuilder builder = TupleId.builder();
                if (context != null && context.getDistribution() != null) {
                    builder.partId(context.getDistribution().getId()).tuple(tuple).indexId(context.getIndexId());
                }
                TupleId tupleId = builder.tuple(tuple).build();
                param.getTupleList().add(tupleId);
                if (param.getTupleList().size() >= SEND_BATCH_SIZE) {
                    return sendTupleList(param);
                }
                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void fin(Fin fin, Vertex vertex) {
        try {
            SendParam param = vertex.getParam();
            SendEndpoint endpoint = param.getEndpoint();
            BufferOutputStream bos = endpoint.getOutputStream(param.getMaxBufferSize());
            param.getCodec().encodeFin(bos, fin);
            if (!(fin instanceof FinWithException)) {
                sendTupleList(param);
            }
            if (log.isDebugEnabled()) {
                log.debug("Send FIN with detail:\n{}", fin.detail());
            }
            endpoint.send(bos, true);
        } catch (IOException e) {
            log.error("Encode FIN failed. fin = {}", fin, e);
        }
    }

    private boolean sendTupleList(SendParam param) throws IOException {
        SendEndpoint endpoint = param.getEndpoint();
        int maxBufferSize = param.getMaxBufferSize();
        List<TupleId> tupleList = param.getTupleList();
        if (!tupleList.isEmpty()) {
            BufferOutputStream bos = endpoint.getOutputStream(maxBufferSize);
            if (tupleList.get(0).getPartId() != null) {
                param.getCodec().encodeTupleIds(bos, tupleList);
            } else {
                param.getCodec().encodeTuples(bos, tupleList.stream().map(TupleId::getTuple).collect(Collectors.toList()));
            }
            if (bos.bytes() > maxBufferSize) {
                param.setMaxBufferSize(bos.bytes());
            }
            boolean result = endpoint.send(bos);
            tupleList.clear();
            return result;
        }
        return true;
    }
}
