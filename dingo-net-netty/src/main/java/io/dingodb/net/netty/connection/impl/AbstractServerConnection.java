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
import lombok.Getter;
import lombok.experimental.Accessors;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

@Accessors(fluent = true)
public abstract class AbstractServerConnection implements Connection {

    protected final ConcurrentHashMap<Long, Channel> channels = new ConcurrentHashMap<>();
    @Getter
    protected final Channel channel;
    @Getter
    protected final Location remoteLocation;
    @Getter
    protected final Location localLocation;

    protected AbstractServerConnection(Location remoteLocation, Location localLocation) {
        this.remoteLocation = remoteLocation;
        this.localLocation = localLocation;
        this.channel = new Channel(0, this);
        this.channels.put(0L, channel);
        Executors.submit(String.format("<%s/%s/server>", remoteLocation.getUrl(), 0L), channel);
    }

    @Override
    public Channel getChannel(long channelId) {
        return channels.get(channelId);
    }

    @Override
    public Channel newChannel(boolean keepAlive) throws InterruptedException {
        throw new UnsupportedOperationException("Server connection cannot create new channel.");
    }

    @Override
    public void closeChannel(long channelId) {
        channels.remove(channelId).shutdown();
    }

    @Override
    public ByteBuffer allocMessageBuffer(long channelId, int capacity) {
        return ByteBuffer.allocate(capacity + PrimitiveCodec.LONG_MAX_LEN).put(PrimitiveCodec.encodeVarLong(channelId));
    }

    @Override
    public void receive(ByteBuffer message) {
        if (message == null) {
            return;
        }
        Long channelId = PrimitiveCodec.readVarLong(message);
        Channel channel = channels.get(channelId);
        if (channel == null) {
            channel = new Channel(channelId, this);
            Executors.submit(String.format("<%s/%s/server>", remoteLocation.getUrl(), channel.channelId()), channel);
            channels.put(channelId, channel);
        }
        channel.receive(message);
    }

    @Override
    public void close() {
        channel.close();
        channels.values().forEach(Channel::close);
        channels.clear();
    }

}
