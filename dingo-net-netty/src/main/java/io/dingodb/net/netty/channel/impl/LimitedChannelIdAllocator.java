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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class LimitedChannelIdAllocator<I extends ChannelId<Integer>> implements ChannelIdAllocator<I> {
    private static final Logger logger = LoggerFactory.getLogger(LimitedChannelIdAllocator.class);

    private final int limit;

    private final BlockingQueue<Integer> channelIdsQueue;
    private final ChannelIdProvider<I> channelIdProvider;

    public LimitedChannelIdAllocator(int max, ChannelIdProvider<I> channelIdProvider) {
        channelIdsQueue = new ArrayBlockingQueue<>(max);
        IntStream.range(1, max + 1).forEach(i -> channelIdsQueue.add(i));
        this.channelIdProvider = channelIdProvider;
        this.limit = max;
    }

    public int limit() {
        return limit;
    }

    @Override
    public I alloc() {
        Integer id = channelIdsQueue.poll();
        if (id == null) {
            return null;
        }
        return channelIdProvider.get(id);
    }

    @Override
    public I alloc(long timeout, TimeUnit unit) {
        Integer id;
        try {
            if ((id = channelIdsQueue.poll(timeout, unit)) == null) {
                return null;
            }
        } catch (InterruptedException e) {
            logger.error("Alloc channel id interrupt.");
            return null;
        }
        return channelIdProvider.get(id);
    }

    @Override
    public void release(I channelId) {
        channelIdsQueue.offer(channelId.channelId() + limit);
    }

}
