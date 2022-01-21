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

package io.dingodb.net.netty.handler.handshake;

import io.dingodb.net.Message;
import io.dingodb.net.SimpleMessage;
import io.dingodb.net.netty.Versions;
import io.dingodb.net.netty.channel.ConnectionSubChannel;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.packet.PacketMode;
import io.dingodb.net.netty.packet.PacketType;
import io.dingodb.net.netty.packet.impl.MessagePacket;
import io.dingodb.net.netty.packet.message.ErrorMessage;
import io.dingodb.net.netty.packet.message.GenericTag;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import static io.dingodb.net.NetError.HANDSHAKE;
import static io.dingodb.net.netty.Versions.currentVersion;

@Slf4j
public class ConnectionHandshakeHandler extends SimpleChannelInboundHandler<Packet<Message>> {

    private final Connection<Message> connection;

    public ConnectionHandshakeHandler(Connection<Message> connection) {
        this.connection = connection;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet<Message> msg) throws Exception {
        byte[] content = msg.toBytes();
        ConnectionSubChannel<Message> genericSubChannel = connection.genericSubChannel();
        genericSubChannel.targetChannelId(msg.header().targetChannelId());
        if (Versions.checkCode(content, 1) && currentVersion() == content[0]) {
            handshakeSuccess(genericSubChannel);
        } else {
            handshakeFailed(genericSubChannel, "Version code not support.");
        }
    }

    private void handshakeSuccess(ConnectionSubChannel<Message> channel) {
        reply(channel, true, null);
        connection.nettyChannel().pipeline().remove(this);
    }

    private void handshakeFailed(ConnectionSubChannel<Message> channel, String message) throws Exception {
        reply(channel, false, message);
        connection.close();
    }

    private ErrorMessage failedMessage(String message) {
        return ErrorMessage.builder().error(HANDSHAKE.format(
            connection.localAddress().getAddress().getHostAddress(),
            connection.localAddress().getPort(),
            message
        )).tag(GenericTag.instance()).build();
    }

    private SimpleMessage successMessage() {
        return SimpleMessage.builder().tag(GenericTag.instance()).content("Handshake ack.".getBytes()).build();
    }

    private void reply(ConnectionSubChannel<Message> channel, boolean success, String message) {
        channel.send(MessagePacket.builder()
            .targetChannelId(channel.targetChannelId())
            .channelId(channel.channelId())
            .mode(PacketMode.GENERIC)
            .type(success ? PacketType.ACK : PacketType.HANDSHAKE_ERROR)
            .content(success ? successMessage() : failedMessage(message))
            .build()
        );
    }

}
