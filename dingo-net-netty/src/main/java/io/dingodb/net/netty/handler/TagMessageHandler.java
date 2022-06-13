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

import io.dingodb.common.util.Optional;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.MessageListenerProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

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
        Collection<MessageListener> listeners =
            this.listeners.compute(tag, (t, ps) -> ps == null ? new CopyOnWriteArraySet<>() : ps);
        listeners.add(listener);
    }

    public void removeTagListener(String tag, MessageListener listener) {
        Optional.ofNullable(listeners.get(tag)).ifPresent(ps -> ps.remove(listener));
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

    public void handler(Channel channel, Message message) {
        String tag = message.tag();
        if (tag == null) {
            return;
        }
        Collection<MessageListener> listeners = this.listeners.get(tag);
        if (listeners == null || listeners.isEmpty()) {
            return;
        }
        listeners.forEach(listener -> onTagMessage(channel, message, listener));
        MessageListenerProvider provider = listenerProviders.get(tag);
        if (provider != null) {
            channel.setMessageListener(provider.get());
        }
    }
}
