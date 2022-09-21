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

package io.dingodb.net.netty.connection;

import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.net.netty.api.ApiRegistryImpl;
import io.dingodb.net.netty.api.HandshakeApi;
import io.dingodb.net.netty.channel.Channel;
import io.dingodb.net.netty.handler.ExceptionHandler;
import io.dingodb.net.netty.handler.MessageHandler;
import io.dingodb.net.netty.packet.Command;
import io.dingodb.net.netty.packet.Type;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.net.netty.NetServiceConfiguration.heartbeat;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
@Accessors(fluent = true)
public class ClientConnection extends Connection {

    protected Bootstrap bootstrap;
    protected EventLoopGroup eventLoopGroup;

    public ClientConnection(Location location) {
        super(location, null);
    }

    public void connect() throws InterruptedException {
        bootstrap = new Bootstrap();
        eventLoopGroup = new NioEventLoopGroup(0, Executors.executor(remoteLocation.getUrl() + "/connection"));
        bootstrap
            .channel(NioSocketChannel.class)
            .group(eventLoopGroup)
            .remoteAddress(remoteLocation.toSocketAddress())
            .handler(channelInitializer());
        bootstrap.connect().sync().await();
        handshake();
    }

    private ChannelInitializer<SocketChannel> channelInitializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                socketChannel = ch;
                ch.pipeline()
                    .addLast(new MessageHandler(ClientConnection.this))
                    .addLast(new IdleStateHandler(heartbeat(), 0, 0, SECONDS))
                    .addLast(new ExceptionHandler(ClientConnection.this));
            }
        };
    }

    protected void handshake() throws InterruptedException {
        ApiRegistryImpl.instance().proxy(HandshakeApi.class, channel, heartbeat())
            .handshake(null, HandshakeApi.Handshake.INSTANCE);
        log.info("Connection open, remote: [{}]", remoteLocation.getUrl());
        InetSocketAddress localAddress = socketChannel.localAddress();
        localLocation = new Location(localAddress.getHostName(), localAddress.getPort());
        Executors.scheduleWithFixedDelayAsync(
            String.format("%s-heartbeat", remoteLocation), this::sendHeartbeat, 0, 1, SECONDS
        );
    }

    private void sendHeartbeat() {
        channel.sendAsync(channel.buffer(Type.COMMAND, 1).writeByte(Command.PING.code()));
    }

    @Override
    protected Map<Long, Channel> createChannels() {
        return new ConcurrentHashMap<>();
    }

    @Override
    protected String channelName(String url, long id) {
        return String.format("<%s/%s/client>", url, id);
    }

    @Override
    public void receive(ByteBuffer message) {
        if (message == null) {
            return;
        }
        long channelId = message.getLong();
        Channel channel = getChannel(channelId);
        if (channel == null) {
            log.error("Receive message, channel id is [{}], but not have channel.", channelId);
            return;
        }
        channel.receive(message);
    }

    @Override
    public void close() {
        super.close();
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
        }
        log.info("Connection close, remote: [{}].", remoteLocation.getUrl());
    }

}
