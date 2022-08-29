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
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Accessors(fluent = true)
public class ServerConnection extends Connection {

    public ServerConnection(SocketChannel socketChannel) {
        super(
            new Location(socketChannel.remoteAddress().getHostName(), socketChannel.remoteAddress().getPort()),
            new Location(socketChannel.localAddress().getHostName(), socketChannel.localAddress().getPort())
        );
        this.socketChannel = socketChannel;
    }


    @Override
    protected Map<Long, Channel> createChannels() {
        return new ConcurrentHashMap<>();
    }

    @Override
    protected String channelName(String url, long id) {
        return String.format("<%s/%s/server>",url, id);
    }

    @Override
    public Channel newChannel(boolean keepAlive) {
        throw new UnsupportedOperationException("Server connection cannot create new channel.");
    }

    @Override
    public void receive(ByteBuffer message) {
        if (message == null) {
            return;
        }
        long channelId = message.getLong();
        Channel channel = channels.get(channelId);
        if (channel == null) {
            channel = createChannel(channelId);
            channels.put(channelId, channel);
        }
        channel.receive(message);
    }

}
