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

import com.google.auto.service.AutoService;
import io.dingodb.net.Message;
import io.dingodb.net.netty.api.ApiRegistryImpl;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.channel.ConnectionSubChannel;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.handler.MessageDispatcher;
import io.dingodb.net.netty.handler.MessageHandler;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.packet.PacketMode;
import io.dingodb.net.netty.packet.PacketType;
import io.dingodb.net.netty.packet.impl.MessagePacket;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ApiMessageHandler implements MessageHandler {

    public static final ApiMessageHandler INSTANCE = new ApiMessageHandler();
    private Map<Connection<Message>, Map<ChannelId, CompletableFuture<Packet<Message>>>> ackFuture;

    private ApiMessageHandler() {
        ackFuture = new ConcurrentHashMap<>();
        MessageDispatcher messageDispatcher = MessageDispatcher.instance();
        messageDispatcher.registerHandler(PacketMode.API, PacketType.INVOKE, this);
        messageDispatcher.registerHandler(PacketMode.API, PacketType.RETURN, this);
    }

    public static ApiMessageHandler instance() {
        return INSTANCE;
    }

    private final ApiRegistryImpl apiRegistry = ApiRegistryImpl.instance();

    @Override
    public void handle(Connection<Message> connection, Packet<Message> packet) {
        ConnectionSubChannel<Message> subChannel = connection.getSubChannel(packet.header().channelId());
        if (packet.header().type() == PacketType.INVOKE) {
            apiRegistry.invoke(subChannel, (MessagePacket) packet);
        } else {
            connection.receive(packet);
        }
    }

    @AutoService(MessageHandler.Provider.class)
    public static class Provider implements MessageHandler.Provider {
        private final ApiMessageHandler instance = instance();

        @Override
        public MessageHandler get() {
            return instance;
        }
    }

}
