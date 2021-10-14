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

package io.dingodb.net.netty.packet.message;

import io.dingodb.net.Message;
import io.dingodb.net.SimpleTag;
import io.dingodb.net.Tag;
import io.dingodb.net.netty.Versions;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.packet.PacketType;
import io.dingodb.net.netty.packet.impl.MessagePacket;
import lombok.Getter;
import lombok.NoArgsConstructor;

import static io.dingodb.net.netty.channel.impl.SimpleChannelId.GENERIC_CHANNEL_ID;
import static io.dingodb.net.netty.packet.PacketMode.GENERIC;
import static java.nio.charset.StandardCharsets.UTF_8;

@Getter
@NoArgsConstructor
public class HandshakeMessage implements Message {

    public static final HandshakeMessage INSTANCE = new HandshakeMessage();
    public static final SimpleTag HANDSHAKE_TAG = SimpleTag.builder().tag(GENERIC.name().getBytes(UTF_8)).build();

    private byte version = Versions.currentVersion();
    private byte[] code = Versions.MAGIC_CODE;

    public static Packet<Message> handshakePacket(ChannelId channelId) {
        return handshakePacket(channelId, null);
    }

    public static Packet<Message> handshakePacket(ChannelId channelId, ChannelId targetId) {
        return MessagePacket.builder()
            .channelId(GENERIC_CHANNEL_ID)
            .content(INSTANCE)
            .mode(GENERIC)
            .type(PacketType.HANDSHAKE)
            .targetChannelId(targetId)
            .build();
    }

    @Override
    public Tag tag() {
        return HANDSHAKE_TAG;
    }

    @Override
    public byte[] toBytes() {
        byte[] bytes = new byte[code.length + 1];
        bytes[0] = version;
        System.arraycopy(code, 0, bytes, 1, code.length);
        return bytes;
    }

    @Override
    public Message load(byte[] bytes) {
        version = bytes[0];
        code = new byte[bytes.length - 1];
        System.arraycopy(bytes, 1, code, 0, code.length);
        return this;
    }

}
