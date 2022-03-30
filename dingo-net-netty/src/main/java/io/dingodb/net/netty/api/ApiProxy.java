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

package io.dingodb.net.netty.api;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.error.DingoException;
import io.dingodb.common.util.PreParameters;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.NetAddressProvider;
import io.dingodb.net.NetError;
import io.dingodb.net.SimpleMessage;
import io.dingodb.net.api.annotation.ApiDeclaration;
import io.dingodb.net.netty.Constant;
import io.dingodb.net.netty.NettyNetService;
import io.dingodb.net.netty.NettyNetServiceProvider;
import io.dingodb.net.netty.channel.impl.NetServiceConnectionSubChannel;
import io.dingodb.net.netty.packet.PacketMode;
import io.dingodb.net.netty.packet.PacketType;
import io.dingodb.net.netty.packet.impl.MessagePacket;
import io.dingodb.net.netty.utils.Serializers;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.dingodb.common.error.CommonError.EXEC;
import static io.dingodb.common.error.CommonError.EXEC_INTERRUPT;
import static io.dingodb.common.error.CommonError.EXEC_TIMEOUT;
import static io.dingodb.common.error.DingoError.OK;
import static io.dingodb.common.error.DingoError.UNKNOWN;
import static java.lang.Thread.currentThread;

@Slf4j
public class ApiProxy<T> implements InvocationHandler {

    private static final NettyNetService netService = NettyNetServiceProvider.NET_SERVICE_INSTANCE;

    private final NetAddressProvider netAddressProvider;
    private final T defined;

    public ApiProxy(NetAddressProvider netAddressProvider) {
        this.netAddressProvider = netAddressProvider;
        this.defined = null;
    }

    public ApiProxy(NetAddressProvider netAddressProvider, T defined) {
        this.netAddressProvider = netAddressProvider;
        this.defined = defined;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        ApiDeclaration declaration = method.getAnnotation(ApiDeclaration.class);
        if (declaration == null) {
            if (defined == null) {
                throw new UnsupportedOperationException();
            }
            return method.invoke(defined, args);
        }
        String name = declaration.name();
        if (name.isEmpty()) {
            name = method.toGenericString();
        }
        return invoke(
            name,
            PreParameters.cleanNull(args, Constant.API_EMPTY_ARGS),
            method.getReturnType()
        );
    }

    protected <T> T invoke(
        String name,
        Object[] args,
        Class<T> returnType
    ) throws Throwable {
        NetServiceConnectionSubChannel channel = netService.newChannel(netAddressProvider.get());
        MessagePacket packet = generatePacket(channel, name, args);
        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        channel.registerMessageListener(callHandler(future));
        try {
            channel.send(packet);
            ByteBuffer buffer = future.get(5, TimeUnit.SECONDS);
            if (buffer.hasRemaining()) {
                return Serializers.read(buffer, returnType);
            } else {
                return null;
            }
        } catch (InterruptedException e) {
            EXEC_INTERRUPT.throwFormatError("invoke api on remote server", currentThread().getName(), e.getMessage());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof DingoException) {
                throw e.getCause();
            }
            EXEC.throwFormatError("invoke api on remote server", currentThread().getName(), e.getMessage());
        } catch (TimeoutException e) {
            EXEC_TIMEOUT.throwFormatError("invoke api on remote server", currentThread().getName(), e.getMessage());
        } finally {
            try {
                channel.close();
            } catch (Exception e) {
                log.error("Close channel error, address: [{}].", channel.remoteAddress(), e);
            }
        }
        throw UNKNOWN.asException();
    }

    private MessagePacket generatePacket(
        NetServiceConnectionSubChannel channel,
        String name,
        Object[] args
    ) throws IOException {
        Message msg;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            outputStream.write(PrimitiveCodec.encodeString(name));
            for (int i = 0; i < args.length; i++) {
                if (args[i] != null) {
                    outputStream.write(PrimitiveCodec.encodeZigZagInt(i));
                    byte[] bytes = Serializers.write(args[i]);
                    outputStream.write(PrimitiveCodec.encodeZigZagInt(bytes.length));
                    outputStream.write(bytes);
                }
            }
            outputStream.flush();
            msg = SimpleMessage.builder().content(outputStream.toByteArray()).build();
        }
        return MessagePacket.builder()
            .channelId(channel.channelId())
            .targetChannelId(channel.targetChannelId())
            .type(PacketType.INVOKE)
            .mode(PacketMode.API)
            .content(msg)
            .msgNo(channel.nextSeq())
            .build();
    }

    private MessageListener callHandler(CompletableFuture<ByteBuffer> future) {
        return (message, channel) -> {
            try {
                ByteBuffer buffer = ByteBuffer.wrap(message.toBytes());
                Integer code = PrimitiveCodec.readZigZagInt(buffer);
                if (OK.getCode() != code) {
                    NetError.valueOf(code).throwError();
                    EXEC.throwFormatError(
                        "invoke remote api",
                        currentThread().getName(),
                        String.format("error code [%s], %s", code, PrimitiveCodec.readString(buffer))
                    );
                }
                future.complete(buffer);
            } catch (Exception e) {
                future.completeExceptionally(e);
            } finally {
                try {
                    channel.close();
                } catch (Exception e) {
                    log.error("Close channel error, address: [{}].", channel.remoteAddress(), e);
                }
            }
        };
    }
}
