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

package io.dingodb.net.netty.connection.impl;

import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.channel.ChannelIdAllocator;
import io.dingodb.net.netty.channel.ConnectionSubChannel;
import io.dingodb.net.netty.channel.impl.NetServiceConnectionSubChannel;
import io.dingodb.net.netty.channel.impl.SimpleChannelId;
import io.dingodb.net.netty.channel.impl.UnlimitedChannelIdAllocator;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.handler.MessageDispatcher;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.packet.impl.MessagePacket;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.net.netty.channel.impl.SimpleChannelId.GENERIC_CHANNEL_ID;

// todo
@Slf4j
public class NetServiceLocalConnection implements Connection<Message> {

    protected final InetSocketAddress socketAddress;
    private final ConcurrentHashMap<ChannelId, NetServiceConnectionSubChannel> subChannels = new ConcurrentHashMap<>();
    protected ConnectionSubChannel<Message> genericSubChannel;
    protected ChannelIdAllocator<SimpleChannelId> channelIdAllocator;

    public NetServiceLocalConnection(int port) {
        genericSubChannel = new NetServiceConnectionSubChannel(GENERIC_CHANNEL_ID, GENERIC_CHANNEL_ID, this);
        socketAddress = new InetSocketAddress("127.0.0.1", port);
        channelIdAllocator = new UnlimitedChannelIdAllocator<>(SimpleChannelId::new);
    }

    private ChannelId generateChannelId() {
        return channelIdAllocator.alloc();
    }

    @Override
    public NetServiceConnectionSubChannel openSubChannel() {
        return openSubChannel(null);
    }

    @Override
    public NetServiceConnectionSubChannel openSubChannel(ChannelId targetChannelId) {
        NetServiceConnectionSubChannel channel = subChannels.computeIfAbsent(
            generateChannelId(),
            cid -> new NetServiceConnectionSubChannel(cid, targetChannelId, this)
        );
        if (log.isDebugEnabled()) {
            log.debug("Open channel from [{}] channel [{}], this channel: [{}]",
                remoteAddress(), targetChannelId, channel.channelId());
        }
        return channel;
    }

    @Override
    public void closeSubChannel(ChannelId channelId) {
        NetServiceConnectionSubChannel subChannel = subChannels.remove(channelId);
        if (subChannel == null || subChannel.status() == Channel.Status.CLOSE) {
            return;
        }
        subChannel.status(Channel.Status.CLOSE);
        subChannel.close();
    }

    @Override
    public NetServiceConnectionSubChannel getSubChannel(ChannelId channelId) {
        return subChannels.get(channelId);
    }

    @Override
    public void open() throws InterruptedException {
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public <S extends Packet<Message>> void send(S packet) {
        ChannelId channelId = packet.header().channelId();
        if (channelId == null) {
            throw new UnsupportedOperationException("Send message must have sub channel.");
        }
        // fdsfsd
        MessagePacket sendPacket = MessagePacket.builder()
            .mode(packet.header().mode())
            .type(packet.header().type())
            .channelId(packet.header().targetChannelId())
            .targetChannelId(packet.header().channelId())
            .content(packet.content())
            .msgNo(packet.header().msgNo())
            .build();
        MessageDispatcher.instance().dispatch(this, sendPacket);
    }

    @Override
    public void receive(Packet<Message> message) {
        ChannelId channelId = message.header().channelId();
        NetServiceConnectionSubChannel channel;
        if (channelId == null || (channel = subChannels.get(channelId)) == null) {
            throw new RuntimeException("Not found channel for channel id: " + channelId);
        }
        channel.receive(message);
    }

    @Override
    public void close() throws Exception {
        subChannels.values().forEach(NetServiceConnectionSubChannel::close);
    }

    @Override
    public io.netty.channel.Channel nettyChannel() {
        return null;
    }

    @Override
    public ByteBufAllocator allocator() {
        return null;
    }

    @Override
    public InetSocketAddress localAddress() {
        return socketAddress;
    }

    @Override
    public InetSocketAddress remoteAddress() {
        return socketAddress;
    }

    @Override
    public ConnectionSubChannel<Message> genericSubChannel() {
        return null;
    }

    public static class Provider implements Connection.Provider {
        @Override
        public NetServiceLocalConnection get(InetSocketAddress remoteAddress) {
            return new NetServiceLocalConnection(remoteAddress.getPort());
        }
    }
}
