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
import io.dingodb.net.NetError;
import io.dingodb.net.netty.connection.impl.NettyClientConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ConnectionManager implements AutoCloseable {

    private final Map<Location, Connection> connections = new ConcurrentHashMap<>(8);

    public Connection open(Location location) {
        if (connections.containsKey(location)) {
            throw new UnsupportedOperationException();
        }
        NettyClientConnection connection = new NettyClientConnection(location);
        try {
            connection.connect();
        } catch (InterruptedException e) {
            NetError.OPEN_CONNECTION_INTERRUPT.throwFormatError(location);
        } catch (Exception e) {
            connection.close();
            throw e;
        }
        connection.socketChannel().closeFuture().addListener(future -> onClose(connection));
        return connection;
    }

    public Connection getOrOpenConnection(Location location) {
        return connections.compute(location, (k, v) -> Optional.ofNullable(v).orElseGet(() -> open(location)));
    }

    public void onOpen(Connection connection) {
        connections.put(connection.remoteLocation(), connection);
    }

    public void onClose(Connection connection) {
        try {
            connections.remove(connection.remoteLocation()).close();
        } catch (Exception e) {
            log.error("Close connection error, remote: [{}]", connection.remoteLocation(), e);
        }
    }

    @Override
    public void close() throws Exception {
        for (Connection connection : connections.values()) {
            connection.close();
        }
    }
}
