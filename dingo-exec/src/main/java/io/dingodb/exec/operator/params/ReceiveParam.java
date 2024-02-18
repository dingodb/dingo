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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.channel.ReceiveEndpoint;
import io.dingodb.exec.codec.TxRxCodec;
import io.dingodb.exec.codec.TxRxCodecImpl;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.OperatorProfile;
import io.dingodb.exec.tuple.TupleId;
import io.dingodb.exec.utils.QueueUtils;
import io.dingodb.exec.utils.TagUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
@Getter
@JsonPropertyOrder({"host", "port", "schema"})
@JsonTypeName("receive")
public class ReceiveParam extends SourceParam {

    private static final int QUEUE_CAPACITY = 1024;

    @JsonProperty("host")
    private final String host;
    @JsonProperty("port")
    private final int port;
    @JsonProperty("schema")
    private final DingoType schema;

    private transient String tag;
    private transient TxRxCodec codec;
    private transient BlockingQueue<TupleId> tupleQueue;
    private transient ReceiveEndpoint endpoint;
    @Setter
    private transient Fin finObj = null;

    public ReceiveParam(
        @JsonProperty("host") String host,
        @JsonProperty("port") int port,
        @JsonProperty("schema") DingoType schema
    ) {
        this.host = host;
        this.port = port;
        this.schema = schema;
    }

    @Override
    public void init(Vertex vertex) {
        codec = new TxRxCodecImpl(schema);
        tupleQueue = new LinkedBlockingDeque<>(QUEUE_CAPACITY);
        tag = TagUtils.tag(vertex.getTask().getJobId(), vertex.getId());
        endpoint = new ReceiveEndpoint(host, port, tag, (byte[] content) -> {
            try {
                List<TupleId> tuples = codec.decode(content);
                for (TupleId tuple : tuples) {
                    if (!endpoint.isStopped() || tuple.getTuple()[0] instanceof Fin) {
                        QueueUtils.forcePut(tupleQueue, tuple);
                    }
                }
            } catch (IOException e) {
                log.error("Exception in receive handler:", e);
            }
        });
        endpoint.init();
        if (log.isDebugEnabled()) {
            log.debug("ReceiveOperator initialized with host={} port={} tag={}", host, port, tag);
        }
    }

    public void addAll(List<OperatorProfile> profiles) {
        getProfiles().addAll(profiles);
    }

    @Override
    public void destroy() {
        if (endpoint != null) {
            endpoint.close();
        }
    }
}
