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

import io.dingodb.common.Location;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.net.MessageListener;
import io.dingodb.net.api.annotation.ApiDeclaration;
import io.dingodb.net.netty.channel.Channel;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.dingodb.net.Message.API_OK;
import static io.dingodb.net.netty.packet.Type.API;

public interface ApiProxy<T> extends InvocationHandler {

    Channel channel();

    T defined();

    int timeout();

    void invoke(Channel ch, ByteBuffer buffer, CompletableFuture<Object> future) throws Exception;

    @Override
    default Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        ApiDeclaration declaration = method.getAnnotation(ApiDeclaration.class);
        if (declaration == null) {
            return invoke(method, args);
        }
        String name = declaration.name();
        if (name.isEmpty()) {
            name = method.toGenericString();
        }
        CompletableFuture<Object> future = new CompletableFuture<>();
        Channel channel = channel();
        try {
            channel.setMessageListener(callHandler(future));
            channel.closeListener(ch -> closeListener(channel, future));
            byte[] nameB = PrimitiveCodec.encodeString(name);
            byte[] content = ProtostuffCodec.write(args);
            invoke(channel, channel.buffer(API, nameB.length + content.length).put(nameB).put(content), future);
        } catch (Exception e) {
            if (channel == null) {
                future.complete(e);
            } else {
                completeExceptionally(future, e, channel.remoteLocation());
            }
        }
        if (method.getReturnType().isInstance(future)) {
            return future;
        }
        int timeout = timeout();
        return timeout == 0 ? future.join() : future.get(timeout, TimeUnit.SECONDS);
    }

    default Object invoke(Method method, Object[] args) throws Exception {
        T defined = defined();
        if (defined == null) {
            throw new UnsupportedOperationException();
        }
        return method.invoke(defined, args);
    }

    static MessageListener callHandler(CompletableFuture<Object> future) {
        return (message, ch) -> {
            try {
                if (message.tag().equals(API_OK)) {
                    future.complete(ProtostuffCodec.read(message.content()));
                } else {
                    completeExceptionally(future, ProtostuffCodec.read(message.content()), ch.remoteLocation());
                }
            } catch (Exception e) {
                completeExceptionally(future, e, ch.remoteLocation());
            }
        };
    }

    static void closeListener(Channel channel, CompletableFuture<Object> future) {
        if (!future.isDone()) {
            completeExceptionally(future, new RuntimeException("Channel closed"), channel.remoteLocation());
        }
    }

    static void completeExceptionally(CompletableFuture<?> future, Throwable throwable, Location location) {
        future.completeExceptionally(
            new InvocationTargetException(throwable, String.format("Invoke on [%s] failed.", location.getUrl()))
        );
    }

}
