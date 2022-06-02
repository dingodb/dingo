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

import io.dingodb.net.netty.channel.Channel;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Accessors(fluent = true)
public class FixedChannelProxy<T> implements InvocationHandler, ApiProxy<T> {

    @Getter
    private final Channel channel;
    @Getter
    private final T defined;
    @Getter
    private final int timeout;

    public FixedChannelProxy(Channel channel) {
        this(channel, null);
    }

    public FixedChannelProxy(Channel channel, T defined) {
        this(channel, defined, 0);
    }

    public FixedChannelProxy(Channel channel, T defined, int timeout) {
        this.channel = channel;
        this.defined = defined;
        this.timeout = timeout;
    }

    @Override
    public void invoke(Channel ch, ByteBuffer buffer, CompletableFuture<Object> future) throws InterruptedException {
        ch.send(buffer);
    }

}
