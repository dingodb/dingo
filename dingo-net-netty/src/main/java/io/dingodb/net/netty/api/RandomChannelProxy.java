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
import io.dingodb.net.netty.NettyNetService;
import io.dingodb.net.netty.NettyNetServiceProvider;
import io.dingodb.net.netty.channel.Channel;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Slf4j
@Accessors(fluent = true)
public class RandomChannelProxy<T> implements InvocationHandler, ApiProxy<T> {

    private static final NettyNetService netService = NettyNetServiceProvider.NET_SERVICE_INSTANCE;

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
    public Channel channel() {
        return netService.newChannel(locationSupplier.get());
    }

    @Override
    public void invoke(Channel ch, ByteBuffer buffer, CompletableFuture<Object> future) throws Exception {
        future.whenComplete((r, e) -> {
            try {
                ch.close();
            } catch (Exception ex) {
                log.error("Close channel error, address: [{}].", ch.remoteLocation(), ex);
            }
        });
        ch.send(buffer);
    }

}
