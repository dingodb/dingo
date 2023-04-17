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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.channel.SendEndpoint;
import io.dingodb.exec.codec.TxRxCodec;
import io.dingodb.exec.codec.TxRxCodecImpl;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.utils.TagUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@Slf4j
@JsonPropertyOrder({"host", "port", "tag", "schema"})
@JsonTypeName("send")
public final class SendOperator extends SinkOperator {
    public static final int SEND_BATCH_SIZE = 256;

    @JsonProperty("host")
    private final String host;
    @JsonProperty("port")
    private final int port;
    @JsonProperty("receiveId")
    private final Id receiveId;
    @JsonProperty("schema")
    private final DingoType schema;
    private final List<Object[]> tupleList;
    private TxRxCodec codec;
    private SendEndpoint endpoint;

    private transient int maxBufferSize;

    @JsonCreator
    public SendOperator(
        @JsonProperty("host") String host,
        @JsonProperty("port") int port,
        @JsonProperty("receiveId") Id receiveId,
        @JsonProperty("schema") DingoType schema
    ) {
        this.host = host;
        this.port = port;
        this.receiveId = receiveId;
        this.schema = schema;
        this.tupleList = new LinkedList<>();
        this.maxBufferSize = 4096;
    }

    @Override
    public void init() {
        super.init();
        codec = new TxRxCodecImpl(schema);
        endpoint = new SendEndpoint(host, port, TagUtils.tag(getTask().getJobId(), receiveId));
        endpoint.init();
    }

    @Override
    public void destroy() {
        safeCloseEndpoint();
    }

    private void safeCloseEndpoint() {
        if (endpoint != null) {
            endpoint.close();
        }
    }

    @Override
    public boolean push(Object[] tuple) {
        try {
            tupleList.add(tuple);
            if (tupleList.size() >= SEND_BATCH_SIZE) {
                return sendTupleList();
            }
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void fin(Fin fin) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            codec.encodeFin(bos, fin);
            if (!(fin instanceof FinWithException)) {
                sendTupleList();
            }
            if (log.isDebugEnabled()) {
                log.debug("Send FIN with detail:\n{}", fin.detail());
            }
            endpoint.send(bos.toByteArray(), true);
        } catch (IOException e) {
            log.error("Encode FIN failed. fin = {}", fin, e);
        }
    }

    private boolean sendTupleList() throws IOException {
        if (!tupleList.isEmpty()) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(maxBufferSize);
            codec.encodeTuples(bos, tupleList);
            if (bos.size() > maxBufferSize) {
                maxBufferSize = bos.size();
            }
            boolean result = endpoint.send(bos.toByteArray());
            tupleList.clear();
            return result;
        }
        return true;
    }
}
