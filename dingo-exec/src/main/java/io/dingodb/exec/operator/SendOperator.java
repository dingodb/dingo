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
import io.dingodb.exec.base.Id;
import io.dingodb.exec.channel.SendEndpoint;
import io.dingodb.exec.codec.AvroTxRxCodec;
import io.dingodb.exec.codec.TxRxCodec;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.util.TagUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;

@Slf4j
@JsonPropertyOrder({"host", "port", "tag", "schema"})
@JsonTypeName("send")
public final class SendOperator extends SinkOperator {
    private static final int SEND_BUFFER_MAX_SIZE = 8192;
    private static final int SEND_BUFFER_HALF_SIZE = (SEND_BUFFER_MAX_SIZE >> 1);
    private static final int SEND_MAX_COUNT = 200;

    @JsonProperty("host")
    private final String host;
    @JsonProperty("port")
    private final int port;
    @JsonProperty("receiveId")
    private final Id receiveId;
    @JsonProperty("schema")
    private final TupleSchema schema;

    private TxRxCodec codec;
    private SendEndpoint endpoint;
    private ByteBuffer sendBuffer;
    private int tupleCount;

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
        this.sendBuffer = ByteBuffer.wrap(new byte[SEND_BUFFER_MAX_SIZE]);
        this.tupleCount = 0;
    }

    @Override
    public void init() {
        super.init();
        codec = new AvroTxRxCodec(schema);
        try {
            endpoint = new SendEndpoint(host, port, TagUtil.tag(getTask().getJobId(), receiveId));
            endpoint.init();
        } catch (Exception e) {
            log.error("Send operator init error, host:{}, port:{}", host, port, e);
        }
    }

    @Override
    public boolean push(@Nonnull Object[] tuple) {
        try {
            byte[] encodeArr = codec.encode(tuple);
            byte[] array = PrimitiveCodec.encodeArray(encodeArr);
            if (log.isDebugEnabled()) {
                log.debug("Will send tuple ({}) to ({}, {}, {}), arr len: {}, total len: {}, buff pos: {}, "
                        + "hashcode: {}", schema.formatTuple(tuple), host, port, receiveId, encodeArr.length,
                    array.length, this.sendBuffer.position(), this.hashCode());
            }

            if (array.length >= SEND_BUFFER_HALF_SIZE) {
                // send data in buffer first
                this.sendBufferData();
                // send directly
                this.endpoint.send(array);
                return true;
            }

            if ((array.length + this.sendBuffer.position() > this.sendBuffer.capacity())
                || this.tupleCount >= SEND_MAX_COUNT) {
                this.sendBufferData();
            }
            this.putArray(array);
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void fin(@Nonnull Fin fin) {
        try {
            if (fin != null && fin instanceof FinWithException) {
                byte[] encodeArr = codec.encodeFin(fin);
                byte[] array = PrimitiveCodec.encodeArray(encodeArr);
                endpoint.send(array);
            } else {
                byte[] encodeArr = codec.encodeFin(fin);
                byte[] array = PrimitiveCodec.encodeArray(encodeArr);
                if ((array.length + this.sendBuffer.position() > this.sendBuffer.capacity())
                    || this.tupleCount >= SEND_MAX_COUNT) {
                    this.sendBufferData();
                }
                this.putArray(array);
                if (log.isDebugEnabled()) {
                    log.debug("Send FIN to ({}, {}, {}), fin length: {}, arr len: {}, buff pos: {}, hashCode: {}.",
                        host, port, receiveId, array.length, encodeArr.length, this.sendBuffer.position(),
                        this.hashCode());
                }
                this.sendBufferData();
            }
            endpoint.close();
        } catch (Exception e) {
            log.error("Send FIN to ({}, {}, {}) error", host, port, receiveId, e);
        }
    }

    private void putArray(byte[] array) {
        this.sendBuffer.put(array);
        this.tupleCount++;
    }

    private void sendBufferData() {
        if (this.tupleCount <= 0) {
            return;
        }
        this.sendBuffer.flip();
        int length = this.sendBuffer.limit() - this.sendBuffer.position();
        if (length <= 0) {
            log.error("Send data to ({}, {}, {}) failed, length: {}.", this.host, this.port, this.receiveId, length);
            return;
        }
        byte[] array = new byte[length];
        this.sendBuffer.get(array);
        this.endpoint.send(array);
        if (log.isDebugEnabled()) {
            log.debug("SendOperator send data to ({}, {}, {}) done, length: {}, tupleCount: {}, hashCode: {}.",
                this.host, this.port, this.receiveId, length, this.tupleCount, this.hashCode());
        }
        this.sendBuffer.clear();
        this.tupleCount = 0;
    }
}
