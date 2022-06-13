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

package io.dingodb.net.netty.connection.impl;

import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.net.netty.NetServiceConfiguration;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class LocalClientConnection extends AbstractClientConnection {

    public static final Location LOCATION = new Location(NetServiceConfiguration.host(), 0);
    public static final LocalClientConnection INSTANCE = new LocalClientConnection();

    private final LocalServerConnection server = LocalServerConnection.INSTANCE;

    public LocalClientConnection() {
        super(LOCATION, LOCATION);
        channel.close();
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public SocketChannel socketChannel() {
        throw new UnsupportedOperationException("Local connection not have socket channel.");
    }

    @Override
    public void send(ByteBuffer message) {
        Executors.execute("local-client-send", () -> server.receive((ByteBuffer) message.flip()));
    }

    @Override
    public void sendAsync(ByteBuffer message) {
        Executors.execute("local-client-send", () -> server.receive((ByteBuffer) message.flip()));
    }

}
