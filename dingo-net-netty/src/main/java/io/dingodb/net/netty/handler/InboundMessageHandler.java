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

package io.dingodb.net.netty.handler;

import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.packet.Packet;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboundMessageHandler extends SimpleChannelInboundHandler<Packet<?>> {

    private final Connection<?> connection;
    private final MessageDispatcher dispatcher;

    public InboundMessageHandler(Connection connection) {
        this.connection = connection;
        dispatcher = MessageDispatcher.instance();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet<?> msg) throws Exception {
        if (!ctx.channel().isOpen()) {
            return;
        }
        try {
            dispatcher.dispatch(connection, msg);
        } catch (Exception e) {
            ctx.fireExceptionCaught(e);
        }
    }
}
