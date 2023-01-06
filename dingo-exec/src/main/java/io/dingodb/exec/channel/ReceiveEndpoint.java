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

package io.dingodb.exec.channel;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.exec.Services;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.atomic.AtomicReference;

import static io.dingodb.exec.Services.CTRL_TAG;

@Slf4j
public class ReceiveEndpoint {
    private final String host;
    private final int port;
    private final String tag;

    private Channel channel;
    private AtomicReference<ControlStatus> emittedStatus;

    public ReceiveEndpoint(String host, int port, String tag) {
        this.host = host;
        this.port = port;
        this.tag = tag;
    }

    public void init() {
        channel = Services.openNewSysChannel(host, port);
        if (log.isDebugEnabled()) {
            log.debug("(tag = {}) Opened channel to {}:{}.", tag, host, port);
        }
        emittedStatus = new AtomicReference<>(ControlStatus.HALT);
    }

    public void sendControlMessage(@NonNull ControlStatus status) {
        switch (status) {
            case READY:
                if (!emittedStatus.compareAndSet(ControlStatus.HALT, ControlStatus.READY)) {
                    return;
                }
                break;
            case HALT:
                if (!emittedStatus.compareAndSet(ControlStatus.READY, ControlStatus.HALT)) {
                    return;
                }
                break;
            case STOP:
                if (emittedStatus.getAndSet(ControlStatus.STOP) == ControlStatus.STOP) {
                    return;
                }
                break;
            default:
                throw new IllegalStateException("Unexpected status: " + status);
        }
        byte[] content;
        try {
            content = ControlMessage.of(tag, status).toBytes();
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize control message: host:{} port:{} tag:{}, status:{}",
                host, port, tag, status, e);
            throw new RuntimeException("Serialize control message failed.", e);
        }
        channel.send(new Message(CTRL_TAG, content));
        if (log.isDebugEnabled()) {
            log.debug("(tag = {}) Sent control message \"{}\".", tag, status);
        }
    }

    public boolean isStopped() {
        return emittedStatus.get() == ControlStatus.STOP;
    }

    public void close() {
        channel.close();
        if (log.isDebugEnabled()) {
            log.debug("(tag = {}) Closed channel to {}:{}.", tag, host, port);
        }
    }
}
