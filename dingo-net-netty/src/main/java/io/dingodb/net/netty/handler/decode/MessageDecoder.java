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

package io.dingodb.net.netty.handler.decode;

import io.dingodb.net.Message;
import io.dingodb.net.RaftTag;
import io.dingodb.net.SimpleMessage;
import io.dingodb.net.SimpleTag;
import io.dingodb.net.Tag;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.channel.impl.SimpleChannelId;
import io.dingodb.net.netty.packet.Header;
import io.dingodb.net.netty.packet.PacketMode;
import io.dingodb.net.netty.packet.PacketType;
import io.dingodb.net.netty.packet.impl.MessagePacket;
import io.dingodb.net.netty.packet.message.GenericTag;
import io.dingodb.net.netty.utils.Serializers;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class MessageDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (!ctx.channel().isOpen()) {
            if (in.readableBytes() > 0) {
                log.info("Channel is closed, discarding remaining {} byte(s) in buffer.", in.readableBytes());
            }
            in.skipBytes(in.readableBytes());
            return;
        }

        ByteBuf messageBuf = readMessageBuf(in);
        if (messageBuf == null) {
            return;
        }

        Header header = readHeader(messageBuf);
        Message message = readMessage(messageBuf);
        out.add(new MessagePacket(header, message));
        messageBuf.release();
    }

    private static Header readHeader(ByteBuf buf) {
        return Header.builder()
            .targetChannelId(readChannelId(buf))
            .channelId(readChannelId(buf))
            .mode(PacketMode.values()[buf.readByte()])
            .type(PacketType.values()[buf.readByte()])
            .msgNo(Serializers.readVarLong(buf))
            .build();
    }

    private static ChannelId readChannelId(ByteBuf buf) {
        Integer len = Serializers.readVarInt(buf);
        if (len == null) {
            throw new CorruptedFrameException("Invalid channel id len: null.");
        }
        if (len == 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return new SimpleChannelId().load(bytes);
    }

    private static Tag readTag(ByteBuf buf) {
        Integer len = Serializers.readVarInt(buf);
        if (len == null) {
            throw new CorruptedFrameException("Invalid length: null.");
        }
        if (len < 0) {
            throw new CorruptedFrameException("Negative length: " + len);
        }
        if (len == 0) {
            return null;
        }
        Integer tagFlag = Serializers.readVarInt(buf);
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return newTagInstance(tagFlag).load(bytes);
    }

    private static Message readMessage(ByteBuf buf) {
        Tag tag = readTag(buf);
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return newMessageInstance(tag).load(bytes);
    }

    private static ByteBuf readMessageBuf(ByteBuf buf) {
        buf.markReaderIndex();
        Integer length;
        if ((length = Serializers.readVarInt(buf)) == null) {
            return null;
        }
        if (length < 0) {
            throw new CorruptedFrameException("Negative length: " + length);
        }
        if (length == 0) {
            throw new CorruptedFrameException("Received a message of length 0.");
        }
        if (!buf.isReadable(length)) {
            buf.resetReaderIndex();
            return null;
        }
        ByteBuf msg = buf.copy(buf.readerIndex(), length);
        buf.skipBytes(length);
        return msg;
    }

    private static Tag newTagInstance(int tagFlag) {
        switch (tagFlag) {
            case 0 :
                return SimpleTag.builder().build();
            case 2 :
                return RaftTag.builder().build();
            default :
                return null;
        }
    }

    private static Message newMessageInstance(Tag tag) {
        return SimpleMessage.builder().tag(tag).build();
    }

}
