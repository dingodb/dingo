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
import io.dingodb.net.netty.handler.TagMessageHandler;
import io.dingodb.net.netty.listener.PortListener;
import io.dingodb.net.netty.listener.impl.NettyServer;
import io.dingodb.net.service.FileTransferService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class NettyNetService implements NetService {

    @Getter
    private final Map<Integer, PortListener> portListeners;
    @Getter
    private final ConnectionManager connectionManager;
    @Getter
    private final String hostname;

    protected NettyNetService() {
        hostname = NetServiceConfiguration.host();
        connectionManager = new ConnectionManager();
        portListeners = new ConcurrentHashMap<>();

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
        FileTransferService.getDefault();
    }

    @Override
    public void cancelPort(int port) throws Exception {
        portListeners.remove(port).close();
    }

    @Override
    public ApiRegistry apiRegistry() {
        return ApiRegistryImpl.instance();
    }

    @Override
    public Channel newChannel(Location location) {
        return newChannel(location, true);
    }

    @Override
    public Channel newChannel(Location location, boolean keepAlive) {
        return connectionManager.getOrOpenConnection(location).newChannel(keepAlive);
    }

    @Override
    public void setMessageListenerProvider(String tag, MessageListenerProvider listenerProvider) {
        PreParameters.nonNull(tag, "tag");
        PreParameters.nonNull(listenerProvider, "listener provider");
        if (log.isDebugEnabled()) {
            log.debug("Register message listener provider, tag: [{}], listener provider class: [{}], caller: [{}]",
                tag,
                listenerProvider.getClass().getName(),
                StackTraces.stack(2));
        }
        TagMessageHandler.INSTANCE.setTagListenerProvider(tag, listenerProvider);
    }

    @Override
    public void unsetMessageListenerProvider(String tag) {
        PreParameters.nonNull(tag, "tag");
        if (log.isDebugEnabled()) {
            log.debug("Unregister message listener provider, tag: [{}], caller: [{}]",
                tag,
                StackTraces.stack(2));
        }
        TagMessageHandler.INSTANCE.unsetTagListenerProvider(tag);
    }

    @Override
    public void registerTagMessageListener(String tag, MessageListener listener) {
        PreParameters.nonNull(tag, "tag");
        PreParameters.nonNull(listener, "listener");
        if (log.isDebugEnabled()) {
            log.info("Register message listener, tag: [{}], listener class: [{}], caller: [{}]",
                tag,
                listener.getClass().getName(),
                StackTraces.stack(2));
        }
        TagMessageHandler.INSTANCE.addTagListener(tag, listener);
    }

    @Override
    public void unregisterTagMessageListener(String tag, MessageListener listener) {
        PreParameters.nonNull(tag, "tag");
        PreParameters.nonNull(listener, "listener");
        if (log.isDebugEnabled()) {
            log.debug("Unregister message listener, tag: [{}], listener class: [{}], caller: [{}]",
                tag,
                listener.getClass().getName(),
                StackTraces.stack(2));
        }
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
