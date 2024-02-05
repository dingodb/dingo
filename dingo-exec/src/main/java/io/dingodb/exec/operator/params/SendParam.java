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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.channel.SendEndpoint;
import io.dingodb.exec.codec.TxRxCodec;
import io.dingodb.exec.codec.TxRxCodecImpl;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.tuple.TupleId;
import io.dingodb.exec.utils.TagUtils;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;

@Getter
@JsonPropertyOrder({"host", "port", "tag", "schema"})
@JsonTypeName("send")
public class SendParam extends AbstractParams {

    @JsonProperty("host")
    private final String host;
    @JsonProperty("port")
    private final int port;
    @JsonProperty("receiveId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId receiveId;
    @JsonProperty("schema")
    private final DingoType schema;

    private transient List<TupleId> tupleList;
    private transient TxRxCodec codec;
    private transient SendEndpoint endpoint;

    @Setter
    private transient int maxBufferSize;

    public SendParam(String host, int port, CommonId receiveId, DingoType schema) {
        this.host = host;
        this.port = port;
        this.receiveId = receiveId;
        this.schema = schema;
        this.maxBufferSize = 4096;
    }

    @Override
    public void init(Vertex vertex) {
        tupleList = new LinkedList<>();
        codec = new TxRxCodecImpl(schema);
        endpoint = new SendEndpoint(host, port, TagUtils.tag(vertex.getTask().getJobId(), receiveId));
        endpoint.init();
    }

    @Override
    public void destroy() {
        if (endpoint != null) {
            endpoint.close();
        }
    }
}
