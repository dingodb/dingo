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

package io.dingodb.driver.mysql.netty;

import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.net.netty.Connection;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

@Getter
@Builder
public class MysqlNettyServer {
    public final String host;
    public final int port;
    public static final Map<String, MysqlConnection> connections = new ConcurrentHashMap<>();

    private EventLoopGroup eventLoopGroup;
    private ServerBootstrap server;

    public void start() throws Exception {
        server = new ServerBootstrap();
        eventLoopGroup = new NioEventLoopGroup(2, new ThreadPoolBuilder().name("Netty server " + port).build());
        server
            .channel(NioServerSocketChannel.class)
            .group(eventLoopGroup)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(channelInitializer());
        if (host != null) {
            server.localAddress(host, port);
        } else {
            server.localAddress(port);
        }
        server.bind().sync().await();
    }

    private ChannelInitializer<SocketChannel> channelInitializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                MysqlConnection mysqlConnection = new MysqlConnection(ch);
                ch.closeFuture().addListener(f -> {
                    if (mysqlConnection.getId() != null) {
                        connections.remove(mysqlConnection.getId());
                    }
                }).addListener(f -> mysqlConnection.close());
                ch.pipeline().addLast("handshake", new HandshakeHandler(mysqlConnection));
                ch.pipeline().addLast("decoder", new MysqlDecoder());
                MysqlIdleStateHandler mysqlIdleStateHandler = new MysqlIdleStateHandler(
                    28800);
                mysqlConnection.mysqlIdleStateHandler = mysqlIdleStateHandler;
                ch.pipeline().addLast("idleStateHandler", mysqlIdleStateHandler);
                ch.pipeline()
                    .addLast("mysqlHandler", new MysqlHandler(mysqlConnection));
            }
        };
    }

    public void close() {
        eventLoopGroup.shutdownGracefully();
    }
}
