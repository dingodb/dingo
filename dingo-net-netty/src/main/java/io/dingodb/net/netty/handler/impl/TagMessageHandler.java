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

package io.dingodb.net.netty.handler.impl;

import io.dingodb.common.util.Optional;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.MessageListenerProvider;
import io.dingodb.net.Tag;
import io.dingodb.net.netty.channel.impl.NetServiceConnectionSubChannel;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.utils.Logs;
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

    private final Map<Tag, Collection<MessageListenerProvider>> listenerProviders = new ConcurrentHashMap<>();

    private final Map<Tag, Collection<MessageListener>> listeners = new ConcurrentHashMap<>();

    public void addTagListenerProvider(Tag tag, MessageListenerProvider listenerProvider) {
        Collection<MessageListenerProvider> providers =
            listenerProviders.compute(tag, (t, ps) -> ps == null ? new CopyOnWriteArraySet<>() : ps);
        providers.add(listenerProvider);
    }

    public void removeTagListenerProvider(Tag tag, MessageListenerProvider listenerProvider) {
        Optional.ofNullable(listenerProviders.get(tag)).ifPresent(ps -> ps.remove(listenerProvider));
    }

    public void addTagListener(Tag tag, MessageListener listener) {
        Collection<MessageListener> listeners =
            this.listeners.compute(tag, (t, ps) -> ps == null ? new CopyOnWriteArraySet<>() : ps);
        listeners.add(listener);
    }

    public void removeTagListener(Tag tag, MessageListener listener) {
        Optional.ofNullable(listeners.get(tag)).ifPresent(ps -> ps.remove(listener));
    }

    public void handler(NetServiceConnectionSubChannel channel, Tag tag, Packet<Message> packet) {
        Collection<MessageListener> listeners = this.listeners.get(packet.content().tag());
        if (listeners == null || listeners.isEmpty()) {
            Logs.packetWarn(
                false,
                log,
                channel.connection(),
                packet,
                "not found listener, tag: " + new String(tag.toBytes())
            );
            return;
        }
        listeners.forEach(listener -> onTagMessage(channel, packet, channel.connection(), listener));
    }

    private void onTagMessage(
        NetServiceConnectionSubChannel channel,
        Packet<Message> packet,
        Connection<Message> connection,
        MessageListener listener
    ) {
        try {
            Optional.ofNullable(listener)
                .ifPresent(listenerT -> listenerT.onMessage(packet.content(), channel))
                .ifPresent(() -> Logs.packetDbg(false, log, connection, packet))
                .ifAbsent(() -> Logs.packetWarn(false, log, connection, packet, listener.getClass() + " return null"));
        } catch (Exception e) {
            Logs.packetErr(false , log, connection, packet, "listener on message error, " + e.getMessage(), e);
        }
    }

    private void onTagMessage(
        NetServiceConnectionSubChannel channel,
        Packet<Message> packet,
        Connection<Message> connection,
        MessageListenerProvider provider
    ) {
        try {
            Optional.ofNullable(provider)
                .map(MessageListenerProvider::get)
                .ifPresent(listener -> listener.onMessage(packet.content(), channel))
                .ifPresent(() -> Logs.packetDbg(false, log, connection, packet))
                .ifAbsent(() -> Logs.packetWarn(false, log, connection, packet, provider.getClass() + " return null"));
        } catch (Exception e) {
            Logs.packetErr(false , log, connection, packet, "listener on message error, " + e.getMessage(), e);
        }
    }

}
