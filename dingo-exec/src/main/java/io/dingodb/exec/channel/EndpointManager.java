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
import io.dingodb.exec.channel.message.Control;
import io.dingodb.exec.channel.message.IncreaseBuffer;
import io.dingodb.exec.channel.message.StopTx;
import io.dingodb.net.Message;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public final class EndpointManager {
    public static EndpointManager INSTANCE = new EndpointManager();

    private final Map<String, SendEndpoint> sendEndpointMap;
    private final Map<String, AtomicInteger> availableBufferCounts;

    private EndpointManager() {
        sendEndpointMap = new ConcurrentHashMap<>();
        availableBufferCounts = new ConcurrentHashMap<>();
    }

    public void onControlMessage(@NonNull Message message) {
        Control msg;
        try {
            msg = Control.fromMessage(message);
        } catch (JsonProcessingException e) {
            log.error("Failed to parse control message", e);
            throw new RuntimeException("Deserializing control message failed.");
        }
        if (log.isDebugEnabled()) {
            log.debug("Received control message {}.", msg);
        }
        String tag = msg.getTag();
        AtomicInteger bufferCount = getBufferCount(tag);
        if (msg instanceof StopTx) {
            bufferCount.set(-1);
        } else if (msg instanceof IncreaseBuffer) {
            bufferCount.getAndAdd(((IncreaseBuffer) msg).getBytes());
        }
        SendEndpoint sendEndpoint = sendEndpointMap.get(tag);
        if (sendEndpoint != null) {
            sendEndpoint.wakeUp();
        }
    }

    public void registerSendEndpoint(SendEndpoint endpoint) {
        sendEndpointMap.put(endpoint.getTag(), endpoint);
    }

    public void unregisterSendEndpoint(@NonNull SendEndpoint endpoint) {
        String tag = endpoint.getTag();
        sendEndpointMap.remove(tag);
        availableBufferCounts.remove(tag);
    }

    AtomicInteger getBufferCount(String tag) {
        return availableBufferCounts.computeIfAbsent(
            tag,
            (t) -> new AtomicInteger(0)
        );
    }
}
