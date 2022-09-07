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
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.net.netty.NetServiceConfiguration;
import io.dingodb.net.netty.channel.Channel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.AttributeMap;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Accessors(fluent = true)
public abstract class Connection  {

    protected final Map<Long, Channel> channels = createChannels();
    protected final AtomicLong channelIdSeq = new AtomicLong(0);
    @Getter
    protected final Channel channel;
    @Getter
    protected Location remoteLocation;
    protected Location localLocation;
    @Getter
    @Delegate(excludes = {ChannelOutboundInvoker.class, AttributeMap.class})
    protected SocketChannel socketChannel;

    public Connection(Location remoteLocation, Location localLocation) {
        this.remoteLocation = remoteLocation;
        this.localLocation = localLocation;
        this.channel = createChannel(0);
        this.channels.put(0L, channel);
    }

    protected abstract Map<Long, Channel> createChannels();

    public abstract void receive(ByteBuffer message);

    public Location localLocation() {
        return DingoConfiguration.instance() == null ? localLocation : DingoConfiguration.location();
    }

    public void send(ByteBuf message) throws InterruptedException {
        socketChannel.writeAndFlush(message).await();
    }

    public void sendAsync(ByteBuf message) {
        socketChannel.writeAndFlush(message);
    }

    /**
     * Two arguments, first is url, second is channel id.
     * @return channel name format
     */
    protected abstract String channelName(String url, long id);

    protected Channel createChannel(long channelId) {
        return new Channel(
            channelId, this, new LinkedRunner(channelName(remoteLocation.getUrl(), channelId)), channels::remove
        );
    }

    public Channel newChannel(boolean keepAlive) {
        return channels.computeIfAbsent(channelIdSeq.incrementAndGet(), this::createChannel);
    }

    public Channel getChannel(long channelId) {
        return channels.get(channelId);
    }

    public void close() {
        channel.shutdown();
        channels.values().forEach(Channel::shutdown);
        channels.clear();
    }

}
