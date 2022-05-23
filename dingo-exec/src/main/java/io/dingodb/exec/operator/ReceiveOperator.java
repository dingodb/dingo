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
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.common.util.Pair;
import io.dingodb.exec.Services;
import io.dingodb.exec.channel.ControlStatus;
import io.dingodb.exec.channel.ReceiveEndpoint;
import io.dingodb.exec.codec.AvroTxRxCodec;
import io.dingodb.exec.codec.TxRxCodec;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.fin.OperatorProfile;
import io.dingodb.exec.util.QueueUtil;
import io.dingodb.exec.util.TagUtil;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
@JsonPropertyOrder({"host", "port", "schema", "output"})
@JsonTypeName("receive")
public final class ReceiveOperator extends SourceOperator {
    @JsonProperty("host")
    private final String host;
    @JsonProperty("port")
    private final int port;
    @JsonProperty("schema")
    private final TupleSchema schema;

    private String tag;
    private TxRxCodec codec;
    private BlockingQueue<Object[]> tupleQueue;
    private ReceiveMessageListener messageListener;
    private ReceiveEndpoint endpoint;
    private Fin finObj;

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
        codec = new AvroTxRxCodec(schema);
        tupleQueue = new LinkedBlockingDeque<>();
        messageListener = new ReceiveMessageListener();
        tag = TagUtil.tag(getTask().getJobId(), getId());
        Services.NET.registerTagMessageListener(
            TagUtil.getTag(tag),
            messageListener
        );
        endpoint = new ReceiveEndpoint(host, port, tag);
        endpoint.init();
        endpoint.sendControlMessage(ControlStatus.READY);
        if (log.isDebugEnabled()) {
            log.debug("ReceiveOperator initialized with host={} port={} tag={}", host, port, tag);
        }
    }

    @Override
    public boolean push() {
        long count = 0;
        OperatorProfile profile = getProfile();
        profile.setStartTimeStamp(System.currentTimeMillis());
        while (true) {
            Object[] tuple = QueueUtil.forceTake(tupleQueue);
            if (!(tuple[0] instanceof Fin)) {
                ++count;
                if (log.isDebugEnabled()) {
                    log.debug("(tag = {}) Take out tuple {} from receiving queue.", tag, schema.formatTuple(tuple));
                }
                output.push(tuple);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("(tag = {}) Take out FIN.", tag);
                }
                profile.setEndTimeStamp(System.currentTimeMillis());
                profile.setProcessedTupleCount(count);
                Fin fin = (Fin) tuple[0];
                if (fin instanceof FinWithProfiles) {
                    profiles.addAll(((FinWithProfiles) fin).getProfiles());
                } else if (fin instanceof FinWithException) {
                    finObj = fin;
                }
                break;
            }
        }
        Services.NET.unregisterTagMessageListener(
            TagUtil.getTag(tag),
            messageListener
        );
        return false;
    }

    @Override
    public void fin(int pin, Fin fin) {
        /**
         * when the upstream operator(`sender`) has failed,
         * then the current operator('receiver`) should failed too
         * so the `Fin` should use FinWithException
         */
        if (finObj != null && finObj instanceof FinWithException) {
            super.fin(pin, finObj);
        } else {
            super.fin(pin, fin);
        }

        try {
            endpoint.close();
        } catch (Exception e) {
            log.error("Fin pin:{} catch exception:{}", pin, e.toString(), e);
        }
    }

    private class ReceiveMessageListener implements MessageListener {
        @Override
        public void onMessage(Message message, Channel channel) {
            try {
                final byte[] content = message.toBytes();
                int count = 0;
                int offset = 0;
                while (offset < content.length) {
                    byte[] bytes;
                    Pair<byte[], Integer> pair = PrimitiveCodec.decodeArray(content, offset);
                    if (pair == null) {
                        log.error("ReceiveMessageListener, parse error.");
                        return;
                    }
                    int arrLen = pair.getKey().length;
                    Object[] tuple = codec.decode(pair.getKey());
                    tuple = schema.convert(tuple);
                    if (log.isDebugEnabled()) {
                        if (!(tuple[0] instanceof Fin)) {
                            log.debug("ReceiveMessageListener (tag = {}) Received tuple {}, arr len: {}, "
                                    + "total len: {}, offset: {}, hashCode: {}.", tag, schema.formatTuple(tuple),
                                arrLen, pair.getValue(), offset, this.hashCode());
                        } else {
                            log.debug("ReceiveMessageListener (tag = {}) Received FIN, arr len: {}, "
                                + "offset: {}, hashCode: {}.", tag, pair.getKey().length, offset, this.hashCode());
                        }
                    }
                    offset += pair.getValue();
                    count++;
                    QueueUtil.forcePut(tupleQueue, tuple);
                }
                if (log.isDebugEnabled()) {
                    log.debug("ReceiveMessageListener onMessage, content length: {}, tupleCount: {}, "
                        + "hashCode: {}.", content.length, count, this.hashCode());
                }
            } catch (IOException e) {
                log.error("ReceiveMessageListener ({}:{} tag = {}) catch exception:{}",
                    host, port, tag, e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
