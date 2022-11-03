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

package io.dingodb.net.netty;

import io.dingodb.common.Location;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.Builder;
import lombok.Getter;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

@Getter
@Builder
public class NettyServer {

    public final int port;
    private final Set<Connection> connections = new CopyOnWriteArraySet<>();

    private EventLoopGroup eventLoopGroup;
    private ServerBootstrap server;

    public void start() throws Exception {
        server = new ServerBootstrap();
        eventLoopGroup = new NioEventLoopGroup(2, new ThreadPoolBuilder().name("Netty server " + port).build());
        server
            .localAddress(port)
            .channel(NioServerSocketChannel.class)
            .group(eventLoopGroup)
            .childHandler(channelInitializer());
        server.bind().sync().await();
    }

    private ChannelInitializer<SocketChannel> channelInitializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                Connection connection = new Connection("<%s/%s/server>", new Location(ch.remoteAddress().getHostName(), ch.remoteAddress().getPort()), ch, true);
                NettyHandlers.initChannelPipelineWithHandshake(ch, connection);
                connections.add(connection);
                ch.closeFuture().addListener(f -> connections.remove(connection)).addListener(f -> connection.close());
            }
        };
    }

    public void close() {
        eventLoopGroup.shutdownGracefully();
    }

}
