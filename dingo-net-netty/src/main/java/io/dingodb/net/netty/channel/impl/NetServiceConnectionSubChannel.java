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

package io.dingodb.net.netty.channel.impl;

import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.Tag;
import io.dingodb.net.netty.channel.AbstractConnectionSubChannel;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.handler.TagMessageHandler;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.packet.PacketMode;
import io.dingodb.net.netty.packet.PacketType;
import io.dingodb.net.netty.packet.impl.MessagePacket;
import io.dingodb.net.netty.utils.Logs;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;

import static io.dingodb.net.netty.channel.impl.SimpleChannelId.GENERIC_CHANNEL_ID;

@Slf4j
public class NetServiceConnectionSubChannel extends AbstractConnectionSubChannel<Message> implements Channel {

    private Status status;
    private Collection<MessageListener> listeners;

    public NetServiceConnectionSubChannel(
        ChannelId channelId,
        ChannelId targetChannelId,
        Connection<Message> connection
    ) {
        super(channelId, targetChannelId, connection);
        if (channelId != GENERIC_CHANNEL_ID) {
            listeners = new CopyOnWriteArraySet<>();
        }
        status = Status.NEW;
    }

    @Override
    public void send(Message sendMsg) {
        MessagePacket packet = MessagePacket.builder()
            .msgNo(nextMsgNo())
            .mode(PacketMode.USER_DEFINE)
            .type(PacketType.USER_DEFINE)
            .channelId(channelId)
            .targetChannelId(targetChannelId)
            .content(sendMsg)
            .build();
        send(packet);
    }

    @Override
    public void registerMessageListener(MessageListener listener) {
        this.listeners.add(listener);
    }

    public NetServiceConnectionSubChannel status(Status status) {
        this.status = status;
        return this;
    }

    @Override
    public Status status() {
        return status;
    }

    @Override
    public InetSocketAddress localAddress() {
        return connection.localAddress();
    }

    @Override
    public InetSocketAddress remoteAddress() {
        return connection.remoteAddress();
    }

    @Override
    public void receive(Packet<Message> packet) {
        Logs.packetDbg(log, connection, packet);
        Tag tag = packet.content().tag();
        if (tag != null) {
            TagMessageHandler.instance().handler(this, tag, packet);
        }
        for (MessageListener listener : listeners) {
            listener.onMessage(packet.content(), this);
        }
    }

    @Override
    public void close() {
        if (status == Status.ACTIVE) {
            send(MessagePacket.disconnectRemoteChannel(channelId, targetChannelId(), nextMsgNo()));
        }
        status = Status.CLOSE;
        connection.closeSubChannel(channelId);
        if (log.isDebugEnabled()) {
            log.debug(
                "Channel [{}/{}] ---> [{}/{}] close.",
                localAddress(), channelId, remoteAddress(), targetChannelId
            );
        }
    }
}
