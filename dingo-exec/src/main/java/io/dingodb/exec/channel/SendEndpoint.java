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

import io.dingodb.exec.Services;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SendEndpoint {
    private final String host;
    private final int port;
    @Getter
    private final String tag;

    private Channel channel;

    public SendEndpoint(String host, int port, String tag) {
        this.host = host;
        this.port = port;
        this.tag = tag;
    }

    public void init() {
        EndpointManager.INSTANCE.registerSendEndpoint(this);
        // This may block.
        channel = Services.openNewChannel(host, port);
        if (log.isInfoEnabled()) {
            log.info("(tag = {}) Opened channel to {}:{}.", tag, host, port);
        }
    }

    synchronized void wakeUp() {
        notify();
    }

    synchronized void checkStatus() {
        ControlStatus status = EndpointManager.INSTANCE.getStatus(tag);
        if (status != ControlStatus.READY) {
            while (true) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    log.warn("Catch (tag = {}) Interrupted while waiting for channel to be ready.", tag);
                    continue;
                }
                status = EndpointManager.INSTANCE.getStatus(tag);
                if (status == ControlStatus.READY) {
                    log.info("ReCheck Status of Instance:{}:{} (tag = {}) Status = {}.",
                        host, port, tag, status);
                    break;
                }
            }
        }
    }

    public void send(byte[] content) {
        checkStatus();
        Message msg = Message.builder()
            .tag(tag)
            .content(content)
            .build();
        channel.send(msg);
    }

    public void close() throws Exception {
        EndpointManager.INSTANCE.unregisterSendEndpoint(this);
        channel.close();
    }
}
