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
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.codec.AvroTxRxCodec;
import io.dingodb.exec.codec.TxRxCodec;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.util.TagUtil;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.SimpleMessage;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import javax.annotation.Nonnull;

@Slf4j
@JsonPropertyOrder({"host", "port", "tag", "schema"})
@JsonTypeName("send")
public final class SendOperator extends SinkOperator {
    // as a flag.
    public static final SendOperator DUMMY = new SendOperator(null, 0, null, null);

    @JsonProperty("host")
    private final String host;
    @JsonProperty("port")
    private final int port;
    @JsonProperty("receiveId")
    private final Id receiveId;
    @JsonProperty("schema")
    private final TupleSchema schema;

    private String tag;
    private Channel channel;
    private TxRxCodec codec;

    @JsonCreator
    public SendOperator(
        @JsonProperty("host") String host,
        @JsonProperty("port") int port,
        @JsonProperty("receiveId") Id receiveId,
        @JsonProperty("schema") TupleSchema schema
    ) {
        super();
        this.host = host;
        this.port = port;
        this.receiveId = receiveId;
        this.schema = schema;
    }

    @Override
    public void init() {
        super.init();
        codec = new AvroTxRxCodec(schema);
        tag = TagUtil.tag(getTask().getJobId(), receiveId);
        try {
            SendOperator so = Services.rcvReadyFlag.putIfAbsent(tag, this);
            if (so == null) {
                // RCV_READY not received.
                synchronized (this) {
                    wait();
                }
            }
            Services.rcvReadyFlag.remove(tag);
            // This may block.
            channel = Services.openNewChannel(host, port);
            if (log.isInfoEnabled()) {
                log.info("(tag = {}) Opened channel to {}:{}.", tag, host, port);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void wakeUp() {
        synchronized (this) {
            notify();
        }
    }

    @Override
    public boolean push(@Nonnull Object[] tuple) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("(tag = {}) Send tuple {}...", tag, schema.formatTuple(tuple));
            }
            Message msg = SimpleMessage.builder()
                .tag(TagUtil.getTag(tag))
                .content(codec.encode(tuple))
                .build();
            channel.send(msg);
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void fin(@Nonnull Fin fin) {
        try {
            Message msg = SimpleMessage.builder()
                .tag(TagUtil.getTag(tag))
                .content(codec.encodeFin(fin))
                .build();
            channel.send(msg);
            channel.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
