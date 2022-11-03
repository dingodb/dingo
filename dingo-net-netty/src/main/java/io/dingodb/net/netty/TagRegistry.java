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

package io.dingodb.net.netty;

import io.dingodb.common.util.DebugLog;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.MessageListenerProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static io.dingodb.common.util.Optional.ifPresent;

@Slf4j
public class TagRegistry {

    protected static final TagRegistry INSTANCE = new TagRegistry();

    protected static void onTagMessage(Message message, Channel channel) {
        INSTANCE.tagMessageListener.onMessage(message, channel);
    }

    private TagRegistry() {
    }

    private final Map<String, MessageListenerProvider> providers = new HashMap<>();

    private final Map<String, Set<MessageListener>> listeners = new HashMap<>();

    public synchronized void setMessageListenerProvider(String tag, MessageListenerProvider provider) {
        Parameters.nonNull(tag, "tag");
        Parameters.nonNull(provider, "listener provider");
        DebugLog.debug(log, "Set message listener provider, tag: [{}], provider : [{}]", tag, provider.getClass());
        providers.put(tag, provider);
    }

    public synchronized void unsetMessageListenerProvider(String tag) {
        Parameters.nonNull(tag, "tag");
        DebugLog.debug(log, "Unset message listener provider, tag: [{}]", tag);
        providers.remove(tag);
    }

    public synchronized void registerTagMessageListener(String tag, MessageListener listener) {
        Parameters.nonNull(tag, "tag");
        Parameters.nonNull(listener, "listener");
        DebugLog.debug(log, "Register message listener, tag: [{}], listener class: [{}]", tag, listener.getClass());
        listeners.compute(
            tag,
            (k, v) -> Optional.ofNullable(v, CopyOnWriteArraySet::new).ifPresent(s -> s.add(listener)).get()
        );
    }

    public synchronized void unregisterTagMessageListener(String tag, MessageListener listener) {
        Parameters.nonNull(tag, "tag");
        Parameters.nonNull(listener, "listener");
        DebugLog.debug(log, "Unregister message listener, tag: [{}], listener : [{}]", tag, listener.getClass());
        listeners.compute(
            tag,
            (k, v) -> Optional.ofNullable(v).ifPresent(s -> s.remove(listener)).filter(s -> !s.isEmpty()).orNull()
        );
    }

    public final MessageListener tagMessageListener = (msg, ch) -> {
        String tag = msg.tag();
        if (tag == null) {
            return;
        }

        ifPresent(providers.get(tag), provider -> ifPresent(provider.get(msg, ch), ch::setMessageListener));

        ifPresent(listeners.get(tag), listeners -> {
            for (MessageListener listener : listeners) {
                try {
                    listener.onMessage(msg, ch);
                } catch (Exception e) {
                    log.error("Execute tag {} message listener error.", msg.tag(), e);
                }
            }
        });
    };

}
