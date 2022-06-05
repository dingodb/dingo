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
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.net.netty.channel.Channel;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.packet.Command;
import io.dingodb.net.netty.packet.Type;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Accessors(fluent = true)
public abstract class AbstractClientConnection implements Connection {

    protected final ConcurrentHashMap<Long, Channel> channels = new ConcurrentHashMap<>();
    protected final AtomicLong channelIdSeq = new AtomicLong(0);
    @Getter
    protected final Channel channel;
    @Getter
    protected Location remoteLocation;
    @Getter
    protected Location localLocation;

    public AbstractClientConnection(Location remoteLocation, Location localLocation) {
        this.remoteLocation = remoteLocation;
        this.localLocation = localLocation;
        this.channel = new Channel(0, this);
        this.channels.put(0L, channel);
        Executors.submit(String.format("<%s/%s/client>", this.remoteLocation.getUrl(), 0L), channel);
    }

    @Override
    public Channel getChannel(long channelId) {
        return channels.get(channelId);
    }

    @Override
    public ByteBuffer allocMessageBuffer(long channelId, int capacity) {
        return ByteBuffer.allocate(capacity + PrimitiveCodec.LONG_MAX_LEN).put(PrimitiveCodec.encodeVarLong(channelId));
    }

    @Override
    public Channel newChannel(boolean keepAlive) {
        Channel channel = channels.computeIfAbsent(channelIdSeq.incrementAndGet(), id -> new Channel(id, this));
        Executors.submit(String.format("<%s/%s/client>", remoteLocation.getUrl(), channel.channelId()), channel);
        return channel;
    }

    @Override
    public void closeChannel(long channelId) {
        Channel channel = channels.remove(channelId);
        if (channel != null) {
            channel.sendAsync(channel.buffer(Type.COMMAND, 1).put(Command.CLOSE.code()));
            channel.shutdown();
        }
    }

    @Override
    public void receive(ByteBuffer message) {
        if (message == null) {
            return;
        }
        getChannel(PrimitiveCodec.readVarLong(message)).receive(message);
    }

    @Override
    public void close() {
        channel.shutdown();
        channels.values().forEach(Channel::shutdown);
        channels.clear();
    }

}
