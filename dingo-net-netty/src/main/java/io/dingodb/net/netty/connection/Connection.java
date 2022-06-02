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
import io.dingodb.net.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;

import java.nio.ByteBuffer;

public interface Connection extends AutoCloseable {

    /**
     * Return {@code true} if this connection is active.
     */
    boolean isActive();

    /**
     * Return this connection netty channel.
     */
    SocketChannel socketChannel();

    /**
     * Returns the local location where this connection is connected to.
     */
    Location localLocation();

    /**
     * Returns the remote location where this connection is connected to.
     */
    Location remoteLocation();

    /**
     * Returns generic sub channel.
     */
    Channel channel();

    ByteBuffer allocMessageBuffer(long channelId, int capacity);

    /**
     * Returns a new sub channel for current connection.
     */
    Channel newChannel(boolean keepAlive) throws InterruptedException;

    /**
     * Close sub channel with the specified {@code channel id}.
     */
    void closeChannel(long channelId);

    /**
     * Returns channel by channel id, if channel id is null or channel not exists, will return null.
     */
    Channel getChannel(long channelId);

    /**
     * Send message to remote-end.
     */
    void send(ByteBuffer message) throws InterruptedException;

    /**
     * Async send message to remote-end.
     */
    void sendAsync(ByteBuffer message);

    /**
     * Receive message.
     */
    void receive(ByteBuffer message);

    @Override
    default void close() {

    }
}
