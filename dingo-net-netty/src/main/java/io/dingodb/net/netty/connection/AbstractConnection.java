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
import io.dingodb.common.concurrent.Executors;
import io.dingodb.net.netty.channel.Channel;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Accessors(fluent = true)
public abstract class AbstractConnection implements Connection {

    protected final ConcurrentHashMap<Long, Channel> channels = new ConcurrentHashMap<>();
    protected final AtomicLong channelIdSeq = new AtomicLong(0);
    @Getter
    protected final Channel channel;
    @Getter
    protected Location remoteLocation;
    @Getter
    protected Location localLocation;

    public AbstractConnection(Location remoteLocation, Location localLocation) {
        this.remoteLocation = remoteLocation;
        this.localLocation = localLocation;
        this.channel = createChannel(0);
        this.channels.put(0L, channel);
        Executors.submit(String.format(submitChannelName(), this.remoteLocation.getUrl(), 0L), channel);
    }

    protected abstract Channel createChannel(long channelId);

    protected abstract String submitChannelName();

    @Override
    public Channel newChannel(boolean keepAlive) {
        Channel channel = channels.computeIfAbsent(channelIdSeq.incrementAndGet(), this::createChannel);
        Executors.submit(String.format(submitChannelName(), remoteLocation.getUrl(), channel.channelId()), channel);
        return channel;
    }

    @Override
    public Channel getChannel(long channelId) {
        return channels.get(channelId);
    }

    @Override
    public void close() {
        channel.shutdown();
        channels.values().forEach(Channel::shutdown);
        channels.clear();
    }

}
