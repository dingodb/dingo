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

import io.dingodb.common.config.DingoOptions;
import io.dingodb.common.util.StackTraces;
import io.dingodb.net.Channel;
import io.dingodb.net.MessageListenerProvider;
import io.dingodb.net.NetAddress;
import io.dingodb.net.NetService;
import io.dingodb.net.Tag;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.netty.api.ApiRegistryImpl;
import io.dingodb.net.netty.channel.impl.NetServiceConnectionSubChannel;
import io.dingodb.net.netty.connection.ConnectionManager;
import io.dingodb.net.netty.connection.impl.NetServiceLocalConnection;
import io.dingodb.net.netty.connection.impl.NetServiceNettyConnection;
import io.dingodb.net.netty.handler.impl.TagMessageHandler;
import io.dingodb.net.netty.listener.PortListener;
import io.dingodb.net.netty.listener.impl.NettyServer;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class NettyNetService implements NetService {

    private final Map<Integer, PortListener> portListeners;
    private final ConnectionManager<NetServiceNettyConnection> connectionManager;
    private final NetServiceLocalConnection localConnection;

    private final int capacity;
    private final String hostname;

    protected NettyNetService() {
        capacity = DingoOptions.instance().getQueueCapacity();
        hostname = DingoOptions.instance().getIp();
        connectionManager = new ConnectionManager<>(new NetServiceNettyConnection.Provider(), capacity);
        portListeners = new ConcurrentHashMap<>();
        localConnection = new NetServiceLocalConnection(capacity);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (Exception e) {
                log.error("Close connection error", e);
            }
        }));
    }

    @Override
    public void listenPort(int port) throws Exception {
        if (portListeners.containsKey(port)) {
            return;
        }
        NettyServer server = NettyServer.builder().port(port).connectionManager(connectionManager).build();
        server.init();
        server.start();
        portListeners.put(port, server);
        log.info("Start listening {}.", port);
    }

    @Override
    public void cancelPort(int port) throws Exception {
        portListeners.remove(port).close();
    }

    @Override
    public ApiRegistry apiRegistry() {
        return ApiRegistryImpl.instance();
    }

    private boolean isLocal(NetAddress address) {
        return address.port() == 0 || (portListeners.containsKey(address.port())
            && address.address().getHostName().equals(hostname));
    }

    @Override
    public NetServiceConnectionSubChannel newChannel(NetAddress netAddress) {
        return (NetServiceConnectionSubChannel) newChannel(netAddress, true);
    }

    @Override
    public Channel newChannel(NetAddress netAddress, boolean keepAlive) {
        if (keepAlive) {
            if (isLocal(netAddress)) {
                return new NetServiceLocalConnection(capacity).openSubChannel(true);
            }
            return connectionManager.getOrOpenConnection(netAddress.address()).openSubChannel(true);
        } else {
            if (isLocal(netAddress)) {
                return localConnection.getChannelPool().poll();
            }
            return connectionManager.getOrOpenConnection(netAddress.address()).getChannelPool().poll();
        }
    }

    @Override
    public void registerMessageListenerProvider(Tag tag, MessageListenerProvider listenerProvider) {
        log.info("Register message listener provider, tag: [{}], listener provider class: [{}], caller: [{}]",
            new String(tag.toBytes()),
            listenerProvider.getClass().getName(),
            StackTraces.stack(2));
        TagMessageHandler.INSTANCE.addTagListenerProvider(tag, listenerProvider);
    }

    @Override
    public void unregisterMessageListenerProvider(Tag tag, MessageListenerProvider listenerProvider) {
        log.info("Unregister message listener provider, tag: [{}], listener provider class: [{}], caller: [{}]",
            new String(tag.toBytes()),
            listenerProvider.getClass().getName(),
            StackTraces.stack(2));
        TagMessageHandler.INSTANCE.removeTagListenerProvider(tag, listenerProvider);
    }

    @Override
    public void close() throws Exception {
        for (PortListener listener : portListeners.values()) {
            listener.close();
        }
        connectionManager.close();
    }
}
