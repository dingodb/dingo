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
import io.dingodb.net.NetError;
import io.dingodb.net.netty.channel.AbstractConnectionSubChannel;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.channel.ConnectionSubChannel;
import io.dingodb.net.netty.channel.impl.LimitedChannelIdAllocator;
import io.dingodb.net.netty.channel.impl.NetServiceConnectionSubChannel;
import io.dingodb.net.netty.channel.impl.SimpleChannelId;
import io.dingodb.net.netty.connection.AbstractNettyConnection;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.handler.impl.GenericMessageHandler;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.packet.impl.MessagePacket;
import io.dingodb.net.netty.utils.Logs;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.dingodb.common.util.StackTraces.stack;
import static io.dingodb.common.util.StackTraces.stackTrace;
import static io.dingodb.net.NetError.OPEN_CHANNEL_BUSY;
import static io.dingodb.net.NetError.OPEN_CHANNEL_TIME_OUT;
import static io.dingodb.net.netty.channel.impl.SimpleChannelId.GENERIC_CHANNEL_ID;
import static io.dingodb.net.netty.packet.message.HandshakeMessage.handshakePacket;

@Slf4j
public class NetServiceNettyConnection extends AbstractNettyConnection<Message> {

    private final ConcurrentHashMap<ChannelId, NetServiceConnectionSubChannel> subChannels = new ConcurrentHashMap<>();
    private final ChannelPool channelPool;
    private ThreadPoolExecutor threadPoolExecutor;

    public NetServiceNettyConnection(InetSocketAddress remoteAddress, int capacity) {
        super(remoteAddress);
        genericSubChannel = new NetServiceConnectionSubChannel(GENERIC_CHANNEL_ID, GENERIC_CHANNEL_ID, this);
        channelPool = new ChannelPool(capacity);
        limitChannelIdAllocator = new LimitedChannelIdAllocator<>(capacity, SimpleChannelId::new);
        threadPoolExecutor = new ThreadPoolBuilder()
            .name("Netty-Connection-executor")
            .maximumThreads(Integer.MAX_VALUE)
            .keepAliveSeconds(60L)
            .threadFactory(new ThreadFactoryBuilder()
                .name("Netty-Connection-executor")
                .daemon(true)
                .group(new ThreadGroup("Netty-Connection-executor"))
                .build())
            .build();
    }

    public NetServiceNettyConnection(io.netty.channel.socket.SocketChannel nettyChannel) {
        super(nettyChannel);
        genericSubChannel = new NetServiceConnectionSubChannel(GENERIC_CHANNEL_ID, GENERIC_CHANNEL_ID, this);
        channelPool = null;
        threadPoolExecutor = new ThreadPoolBuilder()
            .name("Netty-Connection-executor")
            .maximumThreads(Integer.MAX_VALUE)
            .keepAliveSeconds(60L)
            .threadFactory(new ThreadFactoryBuilder()
                .name("Netty-Connection-executor")
                .daemon(true)
                .group(new ThreadGroup("Netty-Connection-executor"))
                .build())
            .build();
    }

    private ChannelId generateChannelId(boolean keepAlive) {
        if (keepAlive) {
            return Optional.ofNullable(unLimitChannelIdAllocator.alloc())
                .orElseThrow(OPEN_CHANNEL_BUSY::formatAsException);
        } else {
            return Optional.ofNullable(limitChannelIdAllocator.alloc())
                .orElseThrow(OPEN_CHANNEL_BUSY::formatAsException);
        }
    }

    @Override
    protected void handshake() {
        Future<Packet<Message>> ack = GenericMessageHandler.instance().waitAck(this, genericSubChannel.channelId());
        genericSubChannel.send(handshakePacket(GENERIC_CHANNEL_ID, GENERIC_CHANNEL_ID));
        try {
            genericSubChannel.targetChannelId(ack.get(heartbeat, TimeUnit.SECONDS).header().targetChannelId());
            log.info("Connection open, remote: [{}]", remoteAddress());
        } catch (InterruptedException e) {
            close();
            NetError.OPEN_CONNECTION_INTERRUPT.throwFormatError(remoteAddress());
        } catch (ExecutionException e) {
            close();
            log.error("Open connection error, remote: [{}], caller: [{}]", remoteAddress(), stack(4));
            NetError.UNKNOWN.throwFormatError(e.getMessage());
        } catch (TimeoutException e) {
            close();
            NetError.OPEN_CONNECTION_TIME_OUT.throwFormatError(remoteAddress());
        }
        eventLoopGroup.scheduleAtFixedRate(this::sendHeartbeat, 0, heartbeat / 2, TimeUnit.SECONDS);
    }

    private void sendHeartbeat() {
        genericSubChannel.send(MessagePacket.ping(0));
    }

    @Override
    public void receive(Packet<Message> packet) {
        ChannelId channelId = packet.header().channelId();
        NetServiceConnectionSubChannel channel;
        if (channelId == null || (channel = subChannels.get(channelId)) == null) {
            throw new RuntimeException("Not found channel for channel id: " + channelId);
        }
        Logs.packetDbg(false, log, this, packet);
        channel.receive(packet);
    }

    @Override
    public NetServiceConnectionSubChannel openSubChannel(boolean keepAlive) {
        NetServiceConnectionSubChannel channel = subChannels.computeIfAbsent(
            generateChannelId(keepAlive),
            cid -> new NetServiceConnectionSubChannel(cid, null, this)
        );
        Future<Packet<Message>> ack = GenericMessageHandler.instance().waitAck(this, channel.channelId());
        long seqNo = channel.nextSeq();
        try {
            channel.send(MessagePacket.connectRemoteChannel(channel.channelId(), seqNo));
            channel.targetChannelId(ack.get(heartbeat, TimeUnit.SECONDS).header().targetChannelId());
        } catch (InterruptedException e) {
            log.error("Open channel catch InterruptedException, channel id: [{}], caller: [{}]",
                channel.channelId(), stack(2));
            closeSubChannel(channel.channelId());
            NetError.OPEN_CHANNEL_INTERRUPT.throwFormatError();
        } catch (ExecutionException e) {
            closeSubChannel(channel.channelId());
            log.error("Open channel error, channel id: [{}], caller: [{}]",
                channel.channelId(), stack(2));
            NetError.UNKNOWN.throwFormatError(e.getMessage());
        } catch (TimeoutException e) {
            log.error("Open channel catch TimeOutException, channel id: [{}], caller: [{}]",
                channel.channelId(), stack(2));
            closeSubChannel(channel.channelId());
            OPEN_CHANNEL_TIME_OUT.throwFormatError();
        }
        if (log.isDebugEnabled()) {
            log.debug(
                "Open channel to [{}] channel [{}], this channel: [{}], caller: [{}]",
                remoteAddress(),
                channel.targetChannelId(),
                channel.channelId(),
                log.isTraceEnabled() ? stackTrace() : stack(3)
            );
        }
        channel.start();
        threadPoolExecutor.submit(channel);
        channel.setChannelPool(keepAlive ? null : channelPool);
        return channel;
    }

    @Override
    public ConnectionSubChannel<Message> openSubChannel(ChannelId targetChannelId, boolean keepAlive) {
        NetServiceConnectionSubChannel channel = subChannels.computeIfAbsent(
            generateChannelId(keepAlive),
            cid -> new NetServiceConnectionSubChannel(cid, targetChannelId, this)
        );
        if (log.isDebugEnabled()) {
            log.debug(
                "Open channel from [{}] channel [{}], this channel: [{}], caller: [{}]",
                remoteAddress(),
                targetChannelId,
                channel.channelId(),
                log.isTraceEnabled() ? stackTrace() : stack(3)
            );
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
        if (subChannel.channelPool() == null) {
            subChannel.status(Channel.Status.INACTIVE);
        }
        subChannel.close();
    }

    @Override
    public NetServiceConnectionSubChannel getSubChannel(ChannelId channelId) {
        return subChannels.get(channelId);
    }

    @Override
    public <S extends Packet<Message>> void send(S packet) {
        if (packet.header().channelId() == null) {
            throw new UnsupportedOperationException("Messages can only be sent over sub channel.");
        }
        try {
            this.nettyChannel().writeAndFlush(packet).await();
        } catch (InterruptedException e) {
            log.error("Send msg interrupt.", e);
        }
    }

    @Override
    public void close() {
        if (channelPool != null) {
            channelPool.clear();
        }
        subChannels.values().stream()
            .peek(ch -> ch.setChannelPool(null))
            .map(AbstractConnectionSubChannel::channelId)
            .forEach(this::closeSubChannel);
        super.close();
        threadPoolExecutor.shutdown();
        log.info("Connection close, remote: [{}], [{}]", remoteAddress(), stack(2));
    }

    @Override
    public Connection.ChannelPool getChannelPool() {
        return channelPool;
    }

    public static class Provider implements Connection.Provider {
        @Override
        public NetServiceNettyConnection get(InetSocketAddress remoteAddress, int capacity) {
            return new NetServiceNettyConnection(remoteAddress, capacity);
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
                channel = NetServiceNettyConnection.this.openSubChannel(false);
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
                log.warn("The channel queue:{} recycles the channel:{} interrupt", queue, channel, e);
            }
        }

        @Override
        public void clear() {
            queue.clear();
        }
    }
}
