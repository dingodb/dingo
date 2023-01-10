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

package io.dingodb.net.netty;

import io.dingodb.common.util.Optional;
import io.dingodb.net.netty.api.ApiRegistryImpl;
import io.dingodb.net.netty.api.AuthProxyApi;
import io.dingodb.net.netty.api.HandshakeApi;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public final class NettyHandlers {

    private NettyHandlers() {
    }

    public static void initChannelPipeline(SocketChannel ch, Connection connection) {
        ch.pipeline()
            .addLast(new Decoder())
            .addLast(new MessageHandler(connection))
            .addLast(new ExceptionHandler());
    }

    public static void initChannelPipelineWithHandshake(SocketChannel ch, Connection connection) {
        ch.pipeline()
            .addLast(new Decoder())
            .addLast(new HandshakeHandler(connection))
            .addLast(new AuthHandler(connection))
            .addLast(new MessageHandler(connection))
            .addLast(new ExceptionHandler());
    }


    @Slf4j
    public static class Decoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            if (!ctx.channel().isOpen()) {
                if (in.readableBytes() > 0) {
                    log.info("Channel is closed, discarding remaining {} byte(s) in buffer.", in.readableBytes());
                }
                in.skipBytes(in.readableBytes());
                return;
            }
            Optional.ifPresent(read(in), out::add);
        }

        private static ByteBuffer read(ByteBuf buf) {
            if (buf.readableBytes() < 5) {
                return null;
            }
            buf.markReaderIndex();
            int length = buf.readInt();
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
            ByteBuffer buffer = ByteBuffer.allocate(length);
            buf.readBytes(buffer);
            return (ByteBuffer) buffer.flip();
        }
    }

    @Slf4j
    @AllArgsConstructor
    public static class AuthHandler extends SimpleChannelInboundHandler<ByteBuffer> {

        static {
            ApiRegistryImpl.INSTANCE.register(AuthProxyApi.class, AuthProxyApi.INSTANCE);
        }

        private final Connection connection;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuffer msg) throws Exception {
            try {
                connection.auth(msg);
                ctx.channel().pipeline().remove(this);
            } catch (Exception e) {
                log.error("Handler message from [{}] error.", connection.remote());
                connection.close();
            }
        }
    }

    @Slf4j
    @AllArgsConstructor
    public static class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuffer> {

        static {
            ApiRegistryImpl.INSTANCE.register(HandshakeApi.class, HandshakeApi.INSTANCE);
        }

        private final Connection connection;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuffer msg) throws Exception {
            try {
                connection.handshake(msg);
                ctx.channel().pipeline().remove(this);
            } catch (Exception e) {
                log.error("Handler message from [{}] error.", connection.remote());
                connection.close();
            }
        }
    }

    @Slf4j
    @AllArgsConstructor
    public static class MessageHandler extends SimpleChannelInboundHandler<ByteBuffer> {

        private final Connection connection;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuffer msg) throws Exception {
            try {
                connection.receive(msg);
            } catch (Exception e) {
                log.error("Handler message from [{}] error.", connection.remote());
            }
        }
    }

    public static class ExceptionHandler implements ChannelHandler {
        private static final Logger logger = LoggerFactory.getLogger(ExceptionHandler.class);

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("Net connection error.", cause);
            ctx.channel().close();
        }
    }
}
