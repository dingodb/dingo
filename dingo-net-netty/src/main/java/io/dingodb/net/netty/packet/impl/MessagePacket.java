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

package io.dingodb.net.netty.packet.impl;

import io.dingodb.net.Message;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.packet.AbstractPacket;
import io.dingodb.net.netty.packet.Header;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.packet.PacketMode;
import io.dingodb.net.netty.packet.PacketType;
import io.dingodb.net.netty.packet.message.EmptyMessage;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.slf4j.Logger;

import static io.dingodb.net.netty.channel.impl.SimpleChannelId.GENERIC_CHANNEL_ID;
import static io.dingodb.net.netty.channel.impl.SimpleChannelId.nullChannelId;
import static io.dingodb.net.netty.packet.PacketMode.GENERIC;
import static io.dingodb.net.netty.packet.PacketType.ACK;
import static io.dingodb.net.netty.packet.PacketType.CONNECT_CHANNEL;
import static io.dingodb.net.netty.packet.PacketType.DIS_CONNECT_CHANNEL;
import static io.dingodb.net.netty.packet.PacketType.PING;
import static io.dingodb.net.netty.packet.PacketType.PONG;

@Getter
@Accessors(fluent = true)
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class MessagePacket extends AbstractPacket<Message> {

    private static final Message EMPTY_MESSAGE = EmptyMessage.INSTANCE;

    public MessagePacket(Header header, Message content) {
        super(header, content);
    }

    @Builder
    public MessagePacket(
        long msgNo,
        ChannelId channelId,
        ChannelId targetChannelId,
        PacketType type,
        PacketMode mode,
        Message content
    ) {
        super(
            Header.builder()
                .msgNo(msgNo)
                .channelId(channelId)
                .targetChannelId(targetChannelId)
                .type(type)
                .mode(mode)
                .build(),
            content
        );
    }

    public static MessagePacket connectRemoteChannel(ChannelId channelId, long msgNo) {
        return new MessagePacket(msgNo, channelId, nullChannelId(), CONNECT_CHANNEL, GENERIC, EMPTY_MESSAGE);
    }

    public static MessagePacket disconnectRemoteChannel(ChannelId channelId, ChannelId targetChannelId, long msgNo) {
        return new MessagePacket(msgNo, channelId, targetChannelId, DIS_CONNECT_CHANNEL, GENERIC, EMPTY_MESSAGE);
    }

    public static MessagePacket ping(long msgNo) {
        return new MessagePacket(msgNo, GENERIC_CHANNEL_ID, GENERIC_CHANNEL_ID, PING, GENERIC, EMPTY_MESSAGE);
    }

    public static MessagePacket pong(long msgNo) {
        return new MessagePacket(msgNo, GENERIC_CHANNEL_ID, GENERIC_CHANNEL_ID, PONG, GENERIC, EMPTY_MESSAGE);
    }

    public static MessagePacket ack(ChannelId channelId, ChannelId targetChannelId, long msgNo) {
        return new MessagePacket(msgNo, channelId, targetChannelId, ACK, GENERIC, EMPTY_MESSAGE);
    }

    @Override
    public byte[] toBytes() {
        return content.toBytes();
    }

}
