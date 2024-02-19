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
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.util.Utils;
import io.dingodb.net.Message;
import io.dingodb.net.netty.Channel;
import io.dingodb.net.netty.NetService;
import io.dingodb.net.netty.NetServiceProvider;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static io.dingodb.net.Message.EMPTY;
import static io.dingodb.net.netty.Constant.API_CANCEL;

@Slf4j
@Accessors(fluent = true)
public class RandomChannelProxy<T> implements ApiProxy<T>, InvocationHandler {

    private static final NetService netService = NetServiceProvider.NET_SERVICE_INSTANCE;

    private final Supplier<Location> locationSupplier;
    @Getter
    private final T defined;
    @Getter
    private final int timeout;

    public RandomChannelProxy(Supplier<Location> locationSupplier) {
        this(locationSupplier, null);
    }

    public RandomChannelProxy(Supplier<Location> locationSupplier, T defined) {
        this(locationSupplier, defined, 0);
    }

    public RandomChannelProxy(Supplier<Location> locationSupplier, T defined, int timeout) {
        this.locationSupplier = locationSupplier;
        this.defined = defined;
        this.timeout = timeout;
    }

    @Override
    public void invoke(Channel ch, ByteBuf buffer, CompletableFuture<Object> future) throws Exception {
        future.whenCompleteAsync((r, e) -> {
            if (e instanceof CancellationException) {
                ch.send(new Message(API_CANCEL, EMPTY.content()));
            }
            try {
                ch.close();
            } catch (Exception ex) {
                log.error("Close channel error, address: [{}].", ch.remoteLocation(), ex);
            }
        }, Executors.executor("on-invoke-complete"));
        ch.send(buffer);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Channel channel = netService.newChannel(locationSupplier.get());
        try {
            return ApiProxy.super.invoke(channel, method, args);
        } catch (Exception e) {
            log.error(
                "Invoke proxy method [{}] on [{}/{}] error.",
                method.toGenericString(), channel.remoteLocation(), channel.channelId(), e
            );
            throw Utils.extractThrowable(e);
        }
    }
}
