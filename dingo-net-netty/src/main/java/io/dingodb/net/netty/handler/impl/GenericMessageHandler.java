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
import io.dingodb.common.util.Optional;
import io.dingodb.net.Message;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.channel.ConnectionSubChannel;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.handler.MessageDispatcher;
import io.dingodb.net.netty.handler.MessageHandler;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.packet.PacketMode;
import io.dingodb.net.netty.packet.PacketType;
import io.dingodb.net.netty.packet.impl.MessagePacket;
import io.dingodb.net.netty.packet.message.ErrorMessage;
import io.dingodb.net.netty.utils.Logs;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

@Slf4j
public class GenericMessageHandler implements MessageHandler {

    public static final GenericMessageHandler INSTANCE = new GenericMessageHandler();
    private Map<Connection<Message>, Map<ChannelId, CompletableFuture<Packet<Message>>>> ackFuture;

    private GenericMessageHandler() {
        ackFuture = new ConcurrentHashMap<>();
        MessageDispatcher messageDispatcher = MessageDispatcher.instance();
        messageDispatcher.registerHandler(PacketMode.GENERIC, PacketType.PING, this);
        messageDispatcher.registerHandler(PacketMode.GENERIC, PacketType.PONG, this);
        messageDispatcher.registerHandler(PacketMode.GENERIC, PacketType.ACK, this);
        messageDispatcher.registerHandler(PacketMode.GENERIC, PacketType.CONNECT_CHANNEL, this);
        messageDispatcher.registerHandler(PacketMode.GENERIC, PacketType.DIS_CONNECT_CHANNEL, this);
        messageDispatcher.registerHandler(PacketMode.GENERIC, PacketType.HANDSHAKE_ERROR, this);
    }

    public static GenericMessageHandler instance() {
        return INSTANCE;
    }

    public Future<Packet<Message>> waitAck(Connection<Message> connection, ChannelId channelId) {
        CompletableFuture<Packet<Message>> future = new CompletableFuture<>();
        Map<ChannelId, CompletableFuture<Packet<Message>>> futureMap = ackFuture.computeIfAbsent(
            connection,
            k -> new HashMap<>()
        );
        Optional.ofNullable(futureMap.get(channelId)).ifPresent(f -> f.completeExceptionally(new TimeoutException()));
        futureMap.put(channelId, future);
        return future;
    }

    @Override
    public void handle(Connection<Message> connection, Packet<Message> packet) {
        switch (packet.header().type()) {
            case PING:
                connection.genericSubChannel().send(MessagePacket.pong(0));
                return;
            case PONG:
                break;
            case CONNECT_CHANNEL:
                ConnectionSubChannel<Message> channel =
                    connection.openSubChannel(packet.header().targetChannelId(), true);
                channel.send(MessagePacket.ack(channel.channelId(), channel.targetChannelId(), channel.nextSeq()));
                break;
            case DIS_CONNECT_CHANNEL:
                connection.closeSubChannel(packet.header().channelId());
                break;
            case ACK:
                Optional.ofNullable(ackFuture.get(connection))
                    .map(futureMap -> futureMap.remove(packet.header().channelId()))
                    .ifPresent(future -> future.complete(packet));
                break;
            case HANDSHAKE_ERROR:
                ErrorMessage errorMessage = ErrorMessage.builder().build().load(packet.content().toBytes());
                log.error("Handshake failed, msg is: {}", errorMessage.getDetailMessage());
                try {
                    connection.close();
                } catch (Exception e) {
                    log.error("Connection close error.", e);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + packet.header().type());
        }
    }

    @AutoService(MessageHandler.Provider.class)
    public static class Provider implements MessageHandler.Provider {
        private final GenericMessageHandler instance = instance();

        @Override
        public MessageHandler get() {
            return instance;
        }
    }

}
