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
import io.dingodb.exec.channel.message.Control;
import io.dingodb.exec.channel.message.IncreaseBuffer;
import io.dingodb.exec.channel.message.StopTx;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import static io.dingodb.exec.Services.CTRL_TAG;

@Slf4j
public class ReceiveEndpoint {
    private final String host;
    private final int port;
    private final String tag;

    private Channel channel;

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
    }

    public void close() {
        channel.close();
        if (log.isDebugEnabled()) {
            log.debug("(tag = {}) Closed channel to {}:{}.", tag, host, port);
        }
    }

    public void sendStopTx() {
        StopTx control = new StopTx(tag);
        sendControl(control);
    }

    public void sendIncreaseBuffer(int bytes) {
        IncreaseBuffer control = new IncreaseBuffer(tag, bytes);
        sendControl(control);
    }

    private void sendControl(@NonNull Control control) {
        byte[] content;
        try {
            content = control.toBytes();
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize control message: {}", control);
            throw new RuntimeException("Failed to serialize control message.", e);
        }
        channel.send(new Message(CTRL_TAG, content), false);
        if (log.isDebugEnabled()) {
            log.debug("(tag = {}) Sent control message \"{}\".", tag, control);
        }
    }
}
