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

import io.dingodb.net.Message;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.channel.ConnectionSubChannel;
import io.dingodb.net.netty.packet.Packet;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;

public interface Connection<M> extends AutoCloseable {

    void open() throws InterruptedException;

    /**
     * Return {@code true} if this connection is active.
     */
    boolean isActive();

    /**
     * Return this connection netty channel.
     */
    Channel nettyChannel();

    /**
     * Returns buffer allocator.
     */
    ByteBufAllocator allocator();

    /**
     * Returns the local address.
     */
    InetSocketAddress localAddress();

    /**
     * Returns the remote address where this connection is connected to.
     */
    InetSocketAddress remoteAddress();

    /**
     * Returns generic sub channel.
     */
    ConnectionSubChannel<M> genericSubChannel();

    /**
     * Returns a new sub channel for current connection.
     */
    ConnectionSubChannel<M> openSubChannel(boolean keepAlive);

    /**
     * Returns a new sub channel for current connection.
     */
    ConnectionSubChannel<M> openSubChannel(ChannelId targetChannelId, boolean keepAlive);

    /**
     * Close sub channel with the specified {@code channel id}.
     */
    void closeSubChannel(ChannelId channelId);

    /**
     * Returns sub channel by channel id, if channel id is null or channel not exists, will return null.
     */
    ConnectionSubChannel<M> getSubChannel(ChannelId channelId);

    /**
     * Send handshake packet to remote-end.
     */
    <P extends Packet<M>> void send(P packet);

    /**
     * Receive message.
     */
    void receive(Packet<Message> message);

    ChannelPool getChannelPool();

    interface Provider {

        <C extends Connection<?>> C get(InetSocketAddress remoteAddress, int capacity);
    }

    interface ChannelPool {
        io.dingodb.net.Channel poll();

        void offer(io.dingodb.net.Channel channel);

        void clear();
    }
}
