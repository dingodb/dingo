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

package io.dingodb.net.netty.channel.impl;

import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.channel.ChannelIdAllocator;
import io.dingodb.net.netty.channel.ChannelIdProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

public class LimitedChannelIdAllocator<I extends ChannelId<Integer>> implements ChannelIdAllocator<I> {
    private static final Logger logger = LoggerFactory.getLogger(LimitedChannelIdAllocator.class);

    private final int limit;

    private final Queue<I> channelIdsQueue;

    public LimitedChannelIdAllocator(int max, ChannelIdProvider<I> channelIdProvider) {
        channelIdsQueue = new ConcurrentLinkedQueue<>();
        IntStream.range(1, max + 1).forEach(i -> channelIdsQueue.add(channelIdProvider.get(i)));
        this.limit = max;
    }

    public int limit() {
        return limit;
    }

    @Override
    public I alloc() {
        I id = channelIdsQueue.poll();
        if (id == null) {
            return null;
        }
        return id;
    }

    @Override
    public void release(I channelId) {
        channelIdsQueue.offer(channelId);
    }

}
