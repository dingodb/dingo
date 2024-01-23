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
import io.dingodb.common.util.Optional;
import io.dingodb.net.NetError;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.netty.api.ApiRegistryImpl;
import io.dingodb.net.netty.service.FileReceiver;
import io.dingodb.net.netty.service.ListenService;
import io.dingodb.net.service.FileTransferService;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.common.concurrent.Executors.executor;
import static io.dingodb.common.util.Optional.ifPresent;
import static io.dingodb.net.netty.Constant.CLIENT;

@Slf4j
public class NetService implements io.dingodb.net.NetService {

    @Getter
    private final Map<String, NettyServer> servers = new ConcurrentHashMap<>();
    @Getter
    private final String hostname = NetConfiguration.host();
    @Delegate
    private final TagRegistry tagRegistry = TagRegistry.INSTANCE;
    @Delegate
    private final ApiRegistry apiRegistry = ApiRegistryImpl.INSTANCE;
    private final Map<Location, Connection> connections = new ConcurrentHashMap<>(8);

    protected NetService() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (Exception e) {
                log.error("Close connection error", e);
            }
        }));
        registerTagMessageListener(Constant.FILE_TRANSFER, FileReceiver::onReceive);
        registerTagMessageListener(Constant.LISTENER, ListenService::onListen);
    }

    @Override
    public void listenPort(int port) throws Exception {
        if (servers.containsKey("::" + port)) {
            return;
        }
        NettyServer server = NettyServer.builder().port(port).build();
        server.start();
        servers.put("::" + port, server);
        log.info("Start listening {}.", "::"  + port);
        FileTransferService.getDefault();
    }

    @Override
    public void listenPort(String host, int port) throws Exception {
        if (servers.containsKey(host + ":" + port)) {
            return;
        }
        NettyServer server = NettyServer.builder().host(host).port(port).build();
        server.start();
        servers.put(host + ":" + port, server);
        log.info("Start listening {}.", host + ":" + port);
        FileTransferService.getDefault();
    }

    @Override
    public void disconnect(Location location) {
        connections.remove(location).close();
    }

    @Override
    public void cancelPort(int port) {
        ifPresent(servers.remove("::" + port), NettyServer::close);
    }

    @Override
    public void cancelPort(String host, int port) {
        ifPresent(servers.remove(host + ":" + port), NettyServer::close);
    }

    @Override
    public ApiRegistry apiRegistry() {
        return ApiRegistryImpl.instance();
    }

    @Override
    public Map<String, Object[]> auth(Location location) {
        return connections.get(location).authContent();
    }

    @Override
    public Channel newChannel(Location location) {
        return newChannel(location, true);
    }

    @Override
    public Channel newChannel(Location location, boolean keepAlive) {
        Connection connection = connections.get(location);
        if (connection == null) {
            connection = connect(location);
        }
        return connection.newChannel();
    }

    @Override
    public void close() throws Exception {
        for (NettyServer server : servers.values()) {
            server.close();
        }
        connections.values().forEach(Connection::close);
    }

    private Connection connect(Location location) {
        return connections.computeIfAbsent(location, k -> {
            Optional<Connection> connection = Optional.empty();
            NioEventLoopGroup executor = new NioEventLoopGroup(0, executor(location.url() + "/connection"));
            try {
                Bootstrap bootstrap = new Bootstrap();
                bootstrap
                    .channel(NioSocketChannel.class)
                    .group(executor)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .remoteAddress(location.toSocketAddress())
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            connection.ifAbsentSet(new Connection(CLIENT, location, ch));
                            NettyHandlers.initChannelPipeline(ch, connection.get());
                        }
                    });
                bootstrap.connect().sync().await();
                connection
                    .ifPresent(Connection::handshake).ifPresent(Connection::auth)
                    .ifPresent(() -> log.info("Connection open, remote: [{}].", location))
                    .orElseThrow(() -> new NullPointerException("connection"));
            } catch (InterruptedException e) {
                log.error("Open connection to [{}] interrupted.", location, e);
                connection.ifPresent(Connection::close);
                executor.shutdownGracefully();
                NetError.OPEN_CONNECTION_INTERRUPT.throwFormatError(location);
            } catch (Exception e) {
                log.error("Open connection to [{}] error.", location, e);
                connection.ifPresent(Connection::close);
                executor.shutdownGracefully();
                throw e;
            }
            connection.get().addCloseListener(__ -> executor.shutdownGracefully());
            connection.ifPresent(__ -> __.addCloseListener(___ -> connections.remove(location, __)));
            connection.ifPresent(__ -> __.socket().closeFuture().addListener(ignore -> __.close()));
            return connection.get();
        });
    }

}
