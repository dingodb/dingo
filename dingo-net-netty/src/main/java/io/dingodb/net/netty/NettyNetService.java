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
import io.dingodb.common.util.PreParameters;
import io.dingodb.common.util.StackTraces;
import io.dingodb.net.MessageListener;
import io.dingodb.net.MessageListenerProvider;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.netty.api.ApiRegistryImpl;
import io.dingodb.net.netty.channel.Channel;
import io.dingodb.net.netty.connection.ConnectionManager;
import io.dingodb.net.netty.connection.impl.LocalClientConnection;
import io.dingodb.net.netty.handler.TagMessageHandler;
import io.dingodb.net.netty.listener.PortListener;
import io.dingodb.net.netty.listener.impl.NettyServer;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class NettyNetService implements NetService {

    private final Map<Integer, PortListener> portListeners;
    private final ConnectionManager connectionManager;
    private final LocalClientConnection localConnection;

    private final int capacity;
    private final String hostname;

    protected NettyNetService() {
        capacity = NetServiceConfiguration.queueCapacity();
        hostname = NetServiceConfiguration.host();
        connectionManager = new ConnectionManager(capacity);
        portListeners = new ConcurrentHashMap<>();
        localConnection = LocalClientConnection.INSTANCE;

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

    private boolean isLocal(Location location) {
        return location.port() == 0 || (portListeners.containsKey(location.port())
            && location.host().equals(hostname));
    }

    @Override
    public Channel newChannel(Location location) {
        return newChannel(location, true);
    }

    @Override
    public Channel newChannel(Location location, boolean keepAlive) {
        //if (isLocal(location)) {
        //    return localConnection.newChannel(keepAlive);
        //}
        try {
            return connectionManager.getOrOpenConnection(location).newChannel(keepAlive);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void registerMessageListenerProvider(String tag, MessageListenerProvider listenerProvider) {
        PreParameters.nonNull(tag, "tag");
        PreParameters.nonNull(listenerProvider, "listener provider");
        log.info("Register message listener provider, tag: [{}], listener provider class: [{}], caller: [{}]",
            tag,
            listenerProvider.getClass().getName(),
            StackTraces.stack(2));
        TagMessageHandler.INSTANCE.addTagListenerProvider(tag, listenerProvider);
    }

    @Override
    public void unregisterMessageListenerProvider(String tag, MessageListenerProvider listenerProvider) {
        PreParameters.nonNull(tag, "tag");
        PreParameters.nonNull(listenerProvider, "listener provider");
        log.info("Unregister message listener provider, tag: [{}], listener provider class: [{}], caller: [{}]",
            tag,
            listenerProvider.getClass().getName(),
            StackTraces.stack(2));
        TagMessageHandler.INSTANCE.removeTagListenerProvider(tag, listenerProvider);
    }

    @Override
    public void registerTagMessageListener(String tag, MessageListener listener) {
        PreParameters.nonNull(tag, "tag");
        PreParameters.nonNull(listener, "listener");
        log.info("Register message listener, tag: [{}], listener class: [{}], caller: [{}]",
            tag,
            listener.getClass().getName(),
            StackTraces.stack(2));
        TagMessageHandler.INSTANCE.addTagListener(tag, listener);
    }

    @Override
    public void unregisterTagMessageListener(String tag, MessageListener listener) {
        PreParameters.nonNull(tag, "tag");
        PreParameters.nonNull(listener, "listener");
        log.info("Unregister message listener, tag: [{}], listener class: [{}], caller: [{}]",
            tag,
            listener.getClass().getName(),
            StackTraces.stack(2));
        TagMessageHandler.INSTANCE.removeTagListener(tag, listener);
    }

    @Override
    public void close() throws Exception {
        for (PortListener listener : portListeners.values()) {
            listener.close();
        }
        connectionManager.close();
    }

}
