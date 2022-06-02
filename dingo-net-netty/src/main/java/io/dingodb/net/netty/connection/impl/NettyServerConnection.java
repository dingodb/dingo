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
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.AttributeMap;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
@Accessors(fluent = true)
public class NettyServerConnection extends AbstractServerConnection {

    @Getter
    @Delegate(excludes = {ChannelOutboundInvoker.class, AttributeMap.class})
    protected SocketChannel socketChannel;

    public NettyServerConnection(SocketChannel socketChannel) {
        super(
            new Location(socketChannel.remoteAddress().getHostName(), socketChannel.remoteAddress().getPort()),
            new Location(socketChannel.localAddress().getHostName(), socketChannel.localAddress().getPort())
        );
        this.socketChannel = socketChannel;
    }

    @Override
    public void send(ByteBuffer message) throws InterruptedException {
        socketChannel.writeAndFlush(message.flip()).await();
    }

    @Override
    public void sendAsync(ByteBuffer message) {
        socketChannel.writeAndFlush(message.flip());
    }

}
