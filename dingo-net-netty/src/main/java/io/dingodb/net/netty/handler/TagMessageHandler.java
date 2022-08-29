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

package io.dingodb.net.netty.handler;

import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.MessageListenerProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class TagMessageHandler {

    public static final TagMessageHandler INSTANCE = new TagMessageHandler();

    public static TagMessageHandler instance() {
        return INSTANCE;
    }

    private TagMessageHandler() {
    }

    private final Map<String, MessageListenerProvider> listenerProviders = new ConcurrentHashMap<>();

    private final Map<String, Collection<MessageListener>> listeners = new ConcurrentHashMap<>();

    public void setTagListenerProvider(String tag, MessageListenerProvider listenerProvider) {
        listenerProviders.put(tag, listenerProvider);
    }

    public void unsetTagListenerProvider(String tag) {
        listenerProviders.remove(tag);
    }

    public void addTagListener(String tag, MessageListener listener) {
        this.listeners.compute(tag, (k, v) -> {
            if (v == null) {
                v = new HashSet<>();
            }
            v.add(listener);
            return v;
        });
    }

    public void removeTagListener(String tag, MessageListener listener) {
        listeners.compute(tag, (k, v) -> {
            if (v == null) {
                return null;
            }
            v.remove(listener);
            if (v.isEmpty()) {
                return null;
            } else {
                return v;
            }
        });
    }

    private void onTagMessage(
        Channel channel,
        Message message,
        MessageListener listener
    ) {
        try {
            listener.onMessage(message, channel);
        } catch (Exception e) {
            log.error("Execute tag {} message listener error.", message.tag(), e);
        }
    }

    public void handler(Message message, Channel channel) {
        String tag = message.tag();
        if (tag == null) {
            return;
        }
        MessageListenerProvider provider = listenerProviders.get(tag);
        if (provider != null) {
            MessageListener listener = provider.get(message, channel);
            if (listener != null) {
                channel.setMessageListener(listener);
            }
        }
        Collection<MessageListener> listeners = this.listeners.get(tag);
        if (listeners == null || listeners.isEmpty()) {
            return;
        }
        listeners.forEach(listener -> onTagMessage(channel, message, listener));
    }
}
