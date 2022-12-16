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

package io.dingodb.net.netty.service;

import io.dingodb.common.concurrent.Executors;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class BroadCastService {

    public static BroadCastService instance = new BroadCastService();

    private ConcurrentHashMap<String, ChannelHandlerContext> broadcastMap = new ConcurrentHashMap<>();

    public Map<String, ChannelHandlerContext> getChannels() {
        return broadcastMap;
    }

    public void saveChannel(String string, ChannelHandlerContext ctx) {
        if (broadcastMap == null) {
            broadcastMap = new ConcurrentHashMap<>();
        }
        broadcastMap.put(string, ctx);
    }

    public ChannelHandlerContext getChannel(String key) {
        if (broadcastMap == null || broadcastMap.isEmpty()) {
            return null;
        }
        return broadcastMap.get(key);
    }

    public void start() {
        Executors.scheduleWithFixedDelayAsync("broadcase", this::broadcast, 10, 300, TimeUnit.SECONDS);
    }

    public void broadcast() {
        if (broadcastMap.size() > 0) {
            broadcastMap.forEach((key, value) -> {
               // get client Section

            });
        }
    }

    public void removeChannel(String key) {
        ChannelHandlerContext context = broadcastMap.remove(key);
        context.close();
    }
}
