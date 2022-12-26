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

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.netty.api.ListenerApi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@AutoService(io.dingodb.net.service.ListenerService.class)
public final class ListenerService implements io.dingodb.net.service.ListenerService {

    public static final Map<String, ListenerServer> servers = new ConcurrentHashMap<>();

    @Override
    public ListenFuture listen(Location location, String tag, CommonId resourceId, Consumer<Message> listener) {
        NetService netService = NetService.getDefault();
        Channel channel = netService.newChannel(location);
        try {
            ApiRegistry.getDefault().proxy(ListenerApi.class, channel).listen(null, tag, resourceId);
            channel.setMessageListener((msg, ch) -> listener.accept(msg));
        } catch (Exception e) {
            channel.close();
            throw e;
        }
        return channel::close;
    }

    @Override
    public void registerListenerServer(ListenerServer listenerServer) {
        servers.put(listenerServer.tag(), listenerServer);
    }
}
