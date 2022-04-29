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
import io.dingodb.net.Message;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

@Slf4j
public final class EndpointManager {
    public static EndpointManager INSTANCE = new EndpointManager();

    private final Map<String, SendEndpoint> sendEndpointMap;
    private final Map<String, ControlStatus> signals;

    private EndpointManager() {
        sendEndpointMap = new ConcurrentHashMap<>();
        signals = new ConcurrentHashMap<>();
    }

    public void onControlMessage(@Nonnull Message message) {
        ControlMessage msg;
        try {
            msg = ControlMessage.fromMessage(message);
        } catch (JsonProcessingException e) {
            log.error("Failed to parse control message", e.toString(), e);
            throw new RuntimeException("Deserializing control message failed.");
        }
        String tag = msg.getTag();
        ControlStatus status = msg.getStatus();
        if (log.isDebugEnabled()) {
            log.debug("Received control message \"{}\" of tag {}.", status, tag);
        }
        signals.put(tag, status);
        SendEndpoint sendEndpoint = sendEndpointMap.get(tag);
        if (sendEndpoint != null && status == ControlStatus.READY) {
            sendEndpoint.wakeUp();
        }
    }

    public void registerSendEndpoint(SendEndpoint endpoint) {
        sendEndpointMap.put(endpoint.getTag(), endpoint);
    }

    public ControlStatus getStatus(@Nonnull String tag) {
        return signals.get(tag);
    }
}
