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
import io.dingodb.net.netty.connection.AbstractConnection;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public abstract class AbstractServerConnection extends AbstractConnection {

    protected AbstractServerConnection(Location remoteLocation, Location localLocation) {
        super(remoteLocation, localLocation);
    }

    @Override
    protected String submitChannelName() {
        return "<%s/%s/server>";
    }

    protected Channel createChannel(long channelId) {
        return new Channel(channelId, this, true, () -> channels.remove(channelId));
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
        Long channelId = PrimitiveCodec.readVarLong(message);
        if (channelId == null) {
            log.error("Receive message, but channel id is null.");
            return;
        }
        Channel channel = channels.get(channelId);
        if (channel == null) {
            channel = createChannel(channelId);
            Executors.submit(String.format(submitChannelName(), remoteLocation.getUrl(), channel.channelId()), channel);
            channels.put(channelId, channel);
        }
        channel.receive(message);
    }

}
