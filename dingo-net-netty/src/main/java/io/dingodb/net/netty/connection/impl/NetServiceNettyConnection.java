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

import io.dingodb.common.util.Optional;
import io.dingodb.common.util.StackTraces;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetError;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.channel.ConnectionSubChannel;
import io.dingodb.net.netty.channel.impl.NetServiceConnectionSubChannel;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.dingodb.common.util.StackTraces.stack;
import static io.dingodb.net.NetError.OPEN_CHANNEL_TIME_OUT;
import static io.dingodb.net.netty.channel.impl.SimpleChannelId.GENERIC_CHANNEL_ID;
import static io.dingodb.net.netty.packet.message.HandshakeMessage.handshakePacket;

@Slf4j
public class NetServiceNettyConnection extends AbstractNettyConnection<Message> {

    private final ConcurrentHashMap<ChannelId, NetServiceConnectionSubChannel> subChannels = new ConcurrentHashMap<>();

    public NetServiceNettyConnection(InetSocketAddress remoteAddress) {
        super(remoteAddress);
        genericSubChannel = new NetServiceConnectionSubChannel(GENERIC_CHANNEL_ID, GENERIC_CHANNEL_ID, this);
    }

    public NetServiceNettyConnection(io.netty.channel.socket.SocketChannel nettyChannel) {
        super(nettyChannel);
        genericSubChannel = new NetServiceConnectionSubChannel(GENERIC_CHANNEL_ID, GENERIC_CHANNEL_ID, this);
    }

    private ChannelId generateChannelId() {
        return Optional.ofNullable(channelIdAllocator.alloc()).orElseThrow(OPEN_CHANNEL_TIME_OUT::formatAsException);
    }

    @Override
    protected void handshake() {
        Future<Packet<Message>> ack = GenericMessageHandler.instance().waitAck(this, genericSubChannel.channelId());
        genericSubChannel.send(handshakePacket(GENERIC_CHANNEL_ID, GENERIC_CHANNEL_ID));
        log.info("Connection open, remote: [{}]", remoteAddress());
        try {
            genericSubChannel.targetChannelId(ack.get(heartbeat, TimeUnit.SECONDS).header().targetChannelId());
        } catch (InterruptedException e) {
            close();
            NetError.OPEN_CONNECTION_INTERRUPT.throwError(remoteAddress());
        } catch (ExecutionException e) {
            close();
            log.error("Open connection error, remote: [{}], caller: [{}]", remoteAddress(), stack(4));
            NetError.UNKNOWN.throwError(e.getMessage());
        } catch (TimeoutException e) {
            close();
            NetError.OPEN_CONNECTION_TIME_OUT.throwError(remoteAddress());
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
        Logs.packetDbg(log, this, packet);
        channel.receive(packet);
    }

    @Override
    public NetServiceConnectionSubChannel openSubChannel() {
        NetServiceConnectionSubChannel channel = subChannels.computeIfAbsent(
            generateChannelId(),
            cid -> new NetServiceConnectionSubChannel(cid, null, this)
        );
        Future<Packet<Message>> ack = GenericMessageHandler.instance().waitAck(this, channel.channelId());
        channel.send(MessagePacket.connectRemoteChannel(channel.channelId(), channel.nextMsgNo()));
        try {
            channel.targetChannelId(ack.get(heartbeat, TimeUnit.SECONDS).header().targetChannelId());
        } catch (InterruptedException e) {
            closeSubChannel(channel.channelId());
            NetError.OPEN_CHANNEL_INTERRUPT.throwError();
        } catch (ExecutionException e) {
            closeSubChannel(channel.channelId());
            log.error("Open channel error, channel id: [{}], caller: [{}]",
                channel.channelId(), stack(2));
            NetError.UNKNOWN.throwError(e.getMessage());
        } catch (TimeoutException e) {
            closeSubChannel(channel.channelId());
            OPEN_CHANNEL_TIME_OUT.throwError();
        }
        if (log.isDebugEnabled()) {
            log.debug("Open channel from [{}] channel [{}], this channel: [{}], stacktrace: [{}], caller: [{}]",
                remoteAddress(), channel.targetChannelId(), channel.channelId(), StackTraces.stack(), stack(3));
        }
        channel.status(Channel.Status.ACTIVE);
        return channel;
    }

    @Override
    public ConnectionSubChannel<Message> openSubChannel(ChannelId targetChannelId) {
        NetServiceConnectionSubChannel channel = subChannels.computeIfAbsent(
            generateChannelId(),
            cid -> new NetServiceConnectionSubChannel(cid, targetChannelId, this)
        ).status(Channel.Status.ACTIVE);
        if (log.isDebugEnabled()) {
            log.debug("Open channel from [{}] channel [{}], this channel: [{}], stacktrace: [{}], caller: [{}]",
                remoteAddress(), targetChannelId, channel.channelId(), StackTraces.stack(), stack(3));
        }
        return channel;
    }

    @Override
    public void closeSubChannel(ChannelId channelId) {
        NetServiceConnectionSubChannel subChannel = subChannels.remove(channelId);
        if (subChannel == null || subChannel.status() == Channel.Status.CLOSE) {
            return;
        }
        subChannel.status(Channel.Status.INACTIVE);
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
        subChannels.values().forEach(NetServiceConnectionSubChannel::close);
        super.close();
        log.info("Connection close, remote: [{}], [{}]", remoteAddress(), stack(2));
    }

    public static class Provider implements Connection.Provider {
        @Override
        public NetServiceNettyConnection get(InetSocketAddress remoteAddress) {
            return new NetServiceNettyConnection(remoteAddress);
        }
    }
}
