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

package io.dingodb.net.netty.handler.encode;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.net.Message;
import io.dingodb.net.Tag;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.channel.impl.SimpleChannelId;
import io.dingodb.net.netty.packet.Header;
import io.dingodb.net.netty.packet.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;


@Slf4j
@ChannelHandler.Sharable
public class MessageEncoder extends MessageToMessageEncoder<Packet<Message>> {
    private static byte[] encodeTag(Tag tag) throws IOException {
        if (tag == null) {
            return PrimitiveCodec.encodeVarInt(0);
        }
        ByteArrayOutputStream bais = new ByteArrayOutputStream();
        byte[] tagBytes = tag.toBytes();
        bais.write(PrimitiveCodec.encodeVarInt(tagBytes.length));
        bais.write(tagBytes);
        bais.flush();
        return bais.toByteArray();
    }

    private static byte[] encodeChannelId(ChannelId channelId) {
        return channelId == null ? NULL_CHANNEL_ID_ENCODED_BYTES : channelId.toBytes();
    }

    @Nonnull
    private static byte[] encodeHeader(@Nonnull Header header) throws IOException {
        ByteArrayOutputStream bais = new ByteArrayOutputStream();
        byte[] channelId = encodeChannelId(header.channelId());
        bais.write(PrimitiveCodec.encodeVarInt(channelId.length));
        bais.write(channelId);
        channelId = encodeChannelId(header.targetChannelId());
        bais.write(PrimitiveCodec.encodeVarInt(channelId.length));
        bais.write(channelId);
        bais.write((byte) header.mode().ordinal());
        bais.write((byte) header.type().ordinal());
        bais.write(PrimitiveCodec.encodeVarLong(header.msgNo()));
        bais.flush();
        return bais.toByteArray();
    }

    public static final byte[] NULL_CHANNEL_ID_ENCODED_BYTES = encodeChannelId(SimpleChannelId.nullChannelId());

    @Override
    protected void encode(ChannelHandlerContext ctx, Packet<Message> packet, List<Object> out) throws Exception {

        ByteBuf buffer = ctx.alloc().buffer();

        byte[] header = encodeHeader(packet.header());
        byte[] tag = encodeTag(packet.content().tag());
        byte[] content = packet.toBytes();

        int msgSize = header.length + tag.length + content.length;

        if (log.isTraceEnabled()) {
            log.info(
                "Trace: Encode packet [{}/{}] ------> [{}/{}], mode: [{}], type: [{}], msg no: [{}].",
                ctx.channel().localAddress(),
                packet.header().channelId(),
                ctx.channel().remoteAddress(),
                packet.header().targetChannelId(),
                packet.header().mode(),
                packet.header().type(),
                packet.header().msgNo()
            );
        }
        buffer.writeBytes(PrimitiveCodec.encodeVarInt(msgSize));
        buffer.writeBytes(header);
        buffer.writeBytes(tag);
        buffer.writeBytes(content);
        out.add(buffer);

    }

}
