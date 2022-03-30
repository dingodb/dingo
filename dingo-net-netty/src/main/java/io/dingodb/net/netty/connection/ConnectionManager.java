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

import io.dingodb.net.NetError;
import io.dingodb.net.netty.listener.CloseConnectionListener;
import io.dingodb.net.netty.listener.OpenConnectionListener;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Slf4j
public class ConnectionManager<C extends Connection<?>>
        implements OpenConnectionListener<C>, CloseConnectionListener<C>, AutoCloseable {

    private final Map<InetSocketAddress, C> connections = new ConcurrentHashMap<>(8);
    private final Connection.Provider provider;
    private final int capacity;

    private final Set<OpenConnectionListener<C>> openConnectionListeners = new CopyOnWriteArraySet<>();
    private final Set<CloseConnectionListener<C>> closeConnectionListeners = new CopyOnWriteArraySet<>();

    public ConnectionManager(Connection.Provider provider, int capacity) {
        this.provider = provider;
        this.capacity = capacity;
    }

    public C openConnection(InetSocketAddress address) {
        if (connections.containsKey(address)) {
            throw new UnsupportedOperationException();
        }
        C connection = provider.get(address, capacity);
        try {
            connection.open();
        } catch (InterruptedException e) {
            NetError.OPEN_CONNECTION_INTERRUPT.throwFormatError(address);
        }
        connection.nettyChannel().closeFuture().addListener(future -> onCloseConnection(connection));
        return connection;
    }

    public C getOrOpenConnection(InetSocketAddress address) {
        return connections.compute(address, (k, v) -> Optional.ofNullable(v).orElseGet(() -> openConnection(address)));
    }

    public void registerConnectionOpenListener(OpenConnectionListener<C> listener) {
        openConnectionListeners.add(listener);
    }

    public void registerConnectionCloseListener(CloseConnectionListener<C> listener) {
        closeConnectionListeners.add(listener);
    }

    @Override
    public void onOpenConnection(C connection) {
        connections.put(connection.remoteAddress(), connection);
        openConnectionListeners.parallelStream().forEach(listener -> listener.onOpenConnection(connection));
    }

    @Override
    public void onCloseConnection(C connection) {
        try {
            connections.remove(connection.remoteAddress()).close();
        } catch (Exception e) {
            log.error("Close connection error, remote: [{}]", connection.remoteAddress(), e);
        }
        closeConnectionListeners.parallelStream().forEach(listener -> listener.onCloseConnection(connection));
    }

    @Override
    public void close() throws Exception {
        for (C c : connections.values()) {
            c.close();
        }
    }
}
