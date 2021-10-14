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
import io.dingodb.common.codec.AvroCodec;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.Services;
import io.dingodb.exec.util.QueueUtil;
import io.dingodb.exec.util.TagUtil;
import io.dingodb.net.Channel;
import io.dingodb.net.MessageListener;
import io.dingodb.net.MessageListenerProvider;
import io.dingodb.net.SimpleMessage;
import io.dingodb.net.SimpleTag;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import javax.annotation.Nonnull;

@Slf4j
@JsonPropertyOrder({"host", "port", "schema"})
@JsonTypeName("receive")
public final class ReceiveOperator extends SourceOperator {
    @JsonProperty("host")
    private final String host;
    @JsonProperty("port")
    private final int port;
    @JsonProperty("schema")
    private final TupleSchema schema;

    private String tag;
    private AvroCodec codec;
    private BlockingQueue<Object[]> tupleQueue;
    private ReceiveMessageListenerProvider messageListenerProvider;

    @JsonCreator
    public ReceiveOperator(
        @JsonProperty("host") String host,
        @JsonProperty("port") int port,
        @JsonProperty("schema") TupleSchema schema
    ) {
        super();
        this.host = host;
        this.port = port;
        this.schema = schema;
    }

    @Override
    public void init() {
        super.init();
        codec = new AvroCodec(schema.getAvroSchema());
        tupleQueue = new LinkedBlockingDeque<>();
        messageListenerProvider = new ReceiveMessageListenerProvider();
        tag = TagUtil.tag(getTask().getJobId(), getId());
        Services.NET.registerMessageListenerProvider(
            TagUtil.getTag(tag),
            messageListenerProvider
        );
        try (Channel channel = Services.openNewSysChannel(host, port)) {
            channel.send(SimpleMessage.builder()
                .tag(SimpleTag.RCV_READY_TAG)
                .content(TagUtil.toBytes(tag))
                .build()
            );
            if (log.isDebugEnabled()) {
                log.debug("(tag = {}) Sent ready signal.", tag);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean push() {
        while (true) {
            Object[] tuple = QueueUtil.forceTake(tupleQueue);
            if (log.isDebugEnabled()) {
                log.debug("(tag = {}) Take out tuple {} from receiving queue.", tag, formatTuple(schema, tuple));
            }
            pushOutput(tuple);
            if (Arrays.equals(tuple, FIN)) {
                break;
            }
        }
        Services.NET.unregisterMessageListenerProvider(
            TagUtil.getTag(tag),
            messageListenerProvider
        );
        return false;
    }

    private class ReceiveMessageListenerProvider implements MessageListenerProvider {
        @Nonnull
        @Override
        public MessageListener get() {
            return (message, channel) -> {
                try {
                    byte[] content = message.toBytes();
                    Object[] tuple = !Arrays.equals(content, FIN_BYTES) ? codec.decode(content) : FIN;
                    if (log.isDebugEnabled()) {
                        log.debug("(tag = {}) Received tuple {}.", tag, formatTuple(schema, tuple));
                    }
                    QueueUtil.forcePut(tupleQueue, tuple);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
        }
    }
}
