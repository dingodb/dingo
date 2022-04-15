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

import io.dingodb.common.concurrent.ThreadFactoryBuilder;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.net.netty.NetServiceConfiguration;
import io.dingodb.net.netty.channel.ChannelIdAllocator;
import io.dingodb.net.netty.channel.ConnectionSubChannel;
import io.dingodb.net.netty.channel.impl.LimitedChannelIdAllocator;
import io.dingodb.net.netty.channel.impl.SimpleChannelId;
import io.dingodb.net.netty.channel.impl.UnlimitedChannelIdAllocator;
import io.dingodb.net.netty.handler.ExceptionHandler;
import io.dingodb.net.netty.handler.InboundMessageHandler;
import io.dingodb.net.netty.handler.decode.MessageDecoder;
import io.dingodb.net.netty.handler.encode.MessageEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public abstract class AbstractNettyConnection<M> implements Connection<M> {

    protected SocketChannel nettyChannel;

    protected InetSocketAddress localAddress;
    protected InetSocketAddress remoteAddress;

    protected Integer heartbeat = NetServiceConfiguration.INSTANCE.getHeartbeat();

    protected Bootstrap bootstrap;
    protected EventLoopGroup eventLoopGroup;

    protected ConnectionSubChannel<M> genericSubChannel;
    protected ChannelIdAllocator<SimpleChannelId> unLimitChannelIdAllocator =
        new UnlimitedChannelIdAllocator<>(SimpleChannelId::new);
    protected ChannelIdAllocator<SimpleChannelId> limitChannelIdAllocator =
        new LimitedChannelIdAllocator<>(NetServiceConfiguration.queueCapacity(), SimpleChannelId::new);

    public AbstractNettyConnection(SocketChannel nettyChannel) {
        this.nettyChannel = nettyChannel;
        this.localAddress = nettyChannel.localAddress();
        this.remoteAddress = nettyChannel.remoteAddress();
    }

    public AbstractNettyConnection(InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    @Override
    public void open() throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolBuilder()
            .name("Netty connection " + remoteAddress)
            .threadFactory(new ThreadFactoryBuilder().name("Netty connection " + remoteAddress).daemon(true).build())
            .build();
        bootstrap = new Bootstrap();
        eventLoopGroup = new NioEventLoopGroup(0, executor);
        bootstrap
            .channel(NioSocketChannel.class)
            .group(eventLoopGroup)
            .remoteAddress(remoteAddress)
            .handler(channelInitializer());
        bootstrap.connect().sync().await();
        localAddress = nettyChannel.localAddress();
        handshake();
    }

    protected abstract void handshake();

    private ChannelInitializer<SocketChannel> channelInitializer() {
        MessageEncoder encoder = new MessageEncoder();

        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                nettyChannel = ch;
                ch.pipeline()
                    .addLast(encoder)
                    .addLast(new MessageDecoder())
                    .addLast(new IdleStateHandler(heartbeat, 0, 0, SECONDS))
                    .addLast(new InboundMessageHandler(AbstractNettyConnection.this))
                    .addLast(new ExceptionHandler(AbstractNettyConnection.this));
            }
        };
    }

    @Override
    public boolean isActive() {
        return nettyChannel.isActive();
    }

    @Override
    public ConnectionSubChannel<M> genericSubChannel() {
        return genericSubChannel;
    }

    @Override
    public Channel nettyChannel() {
        return nettyChannel;
    }

    @Override
    public ByteBufAllocator allocator() {
        return nettyChannel.alloc();
    }

    @Override
    public InetSocketAddress localAddress() {
        return localAddress;
    }

    @Override
    public InetSocketAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public void close() {
        if (nettyChannel.isActive()) {
            nettyChannel.disconnect();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
        }
    }
}
