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

import io.dingodb.common.util.StackTraces;
import io.dingodb.net.Channel;
import io.dingodb.net.MessageListenerProvider;
import io.dingodb.net.NetAddress;
import io.dingodb.net.NetService;
import io.dingodb.net.Tag;
import io.dingodb.net.netty.connection.ConnectionManager;
import io.dingodb.net.netty.connection.impl.NetServiceLocalConnection;
import io.dingodb.net.netty.connection.impl.NetServiceNettyConnection;
import io.dingodb.net.netty.handler.impl.TagMessageHandler;
import io.dingodb.net.netty.listener.PortListener;
import io.dingodb.net.netty.listener.impl.NettyServer;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class NettyNetService implements NetService {

    private final Map<Tag, Collection<MessageListenerProvider>> listenerProviders = new ConcurrentHashMap<>();

    private final Map<Integer, PortListener> portListeners;
    private final ConnectionManager<NetServiceNettyConnection> connectionManager;
    private final Map<Integer, NetServiceLocalConnection> portLocalConnections;

    private final NetServiceConfiguration configuration = NetServiceConfiguration.instance();

    protected NettyNetService() {
        connectionManager = new ConnectionManager<>(new NetServiceNettyConnection.Provider());
        portListeners = new ConcurrentHashMap<>();
        portLocalConnections = new HashMap<>();
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
        portLocalConnections.put(port, new NetServiceLocalConnection(port));
        log.info("Start listening {}.", port);
    }

    private boolean isLocal(InetAddress address) {
        return     address.isLoopbackAddress()
                || address.isLinkLocalAddress()
                || address.isAnyLocalAddress()
                || address.isSiteLocalAddress();
    }

    @Override
    public Channel newChannel(NetAddress netAddress) {
        // todo if (isLocal(address.getAddress()) && portListeners.containsKey(address.getPort())) {
        //          return portLocalConnections.get(address.getPort()).openSubChannel();
        //      }
        return connectionManager.getOrOpenConnection(netAddress.address()).openSubChannel();
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
