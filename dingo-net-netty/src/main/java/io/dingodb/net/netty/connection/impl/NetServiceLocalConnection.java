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

import io.dingodb.common.concurrent.ThreadFactoryBuilder;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.common.util.Optional;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.netty.NetServiceConfiguration;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.channel.ChannelIdAllocator;
import io.dingodb.net.netty.channel.ConnectionSubChannel;
import io.dingodb.net.netty.channel.impl.LimitedChannelIdAllocator;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.dingodb.net.NetError.OPEN_CHANNEL_TIME_OUT;
import static io.dingodb.net.netty.channel.impl.SimpleChannelId.GENERIC_CHANNEL_ID;

@Slf4j
public class NetServiceLocalConnection implements Connection<Message> {

    protected final InetSocketAddress socketAddress;
    private final ConcurrentHashMap<ChannelId, NetServiceConnectionSubChannel> subChannels = new ConcurrentHashMap<>();
    protected ConnectionSubChannel<Message> genericSubChannel;
    protected ChannelIdAllocator<SimpleChannelId> limitChannelIdAllocator;
    protected ChannelIdAllocator<SimpleChannelId> unLimitChannelIdAllocator;
    protected ThreadPoolExecutor threadPoolExecutor;

    private final ChannelPool channelPool;

    public NetServiceLocalConnection(int capacity) {
        genericSubChannel = new NetServiceConnectionSubChannel(GENERIC_CHANNEL_ID, GENERIC_CHANNEL_ID, this);
        socketAddress = new InetSocketAddress(NetServiceConfiguration.host(), 0);
        channelPool = new ChannelPool(capacity);
        limitChannelIdAllocator = new LimitedChannelIdAllocator<>(capacity, SimpleChannelId::new);
        unLimitChannelIdAllocator = new UnlimitedChannelIdAllocator<>(SimpleChannelId::new);
        threadPoolExecutor = new ThreadPoolBuilder()
            .name("Local-Connection-executor")
            .maximumThreads(Integer.MAX_VALUE)
            .keepAliveSeconds(60L)
            .threadFactory(new ThreadFactoryBuilder()
                .name("Local-Connection-executor")
                .daemon(true)
                .group(new ThreadGroup("Local-Connection-executor"))
                .build())
            .build();
    }

    private ChannelId generateChannelId(boolean keepAlive) {
        if (keepAlive) {
            return Optional.ofNullable(unLimitChannelIdAllocator.alloc())
                .orElseThrow(OPEN_CHANNEL_TIME_OUT::formatAsException);
        } else {
            return Optional.ofNullable(limitChannelIdAllocator.alloc())
                .orElseThrow(OPEN_CHANNEL_TIME_OUT::formatAsException);
        }
    }

    @Override
    public NetServiceConnectionSubChannel openSubChannel(boolean keepAlive) {
        return openSubChannel(null, keepAlive);
    }

    @Override
    public NetServiceConnectionSubChannel openSubChannel(ChannelId targetChannelId, boolean keepAlive) {
        NetServiceConnectionSubChannel channel = subChannels.computeIfAbsent(
            generateChannelId(keepAlive),
            cid -> new NetServiceConnectionSubChannel(cid, targetChannelId, this)
        );
        if (log.isDebugEnabled()) {
            log.debug("Open channel from [{}] channel [{}], this channel: [{}]",
                remoteAddress(), targetChannelId, channel.channelId());
        }
        channel.start();
        threadPoolExecutor.submit(channel);
        channel.setChannelPool(keepAlive ? null : channelPool);
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
        MessagePacket sendPacket = MessagePacket.builder()
            .mode(packet.header().mode())
            .type(packet.header().type())
            .channelId(packet.header().channelId())
            .targetChannelId(packet.header().targetChannelId())
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
        if (channelPool != null) {
            for (NetServiceConnectionSubChannel channel : subChannels.values()) {
                channel.setChannelPool(null);
                channel.close();
            }
            channelPool.clear();
        } else {
            subChannels.values().forEach(NetServiceConnectionSubChannel::close);
        }
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

    @Override
    public Connection.ChannelPool getChannelPool() {
        return channelPool;
    }

    public static class Provider implements Connection.Provider {
        @Override
        public NetServiceLocalConnection get(InetSocketAddress remoteAddress, int capacity) {
            return new NetServiceLocalConnection(capacity);
        }
    }

    public class ChannelPool implements Connection.ChannelPool {

        private final LinkedBlockingQueue<Channel> queue;

        public ChannelPool(int capacity) {
            this.queue = new LinkedBlockingQueue<>(capacity);
        }

        @Override
        public Channel poll() {
            Channel channel = queue.poll();
            if (channel == null) {
                channel = NetServiceLocalConnection.this.openSubChannel(false);
            }
            ((NetServiceConnectionSubChannel) channel).status(Channel.Status.ACTIVE);
            threadPoolExecutor.submit((NetServiceConnectionSubChannel) channel);
            return channel;
        }

        @Override
        public void offer(Channel channel) {
            try {
                queue.offer(channel, 3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.warn("The channel connection pool:{} recycles the channel:{} interrupt", queue, channel, e);
            }
        }

        @Override
        public void clear() {
            queue.clear();
        }
    }

}
