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

import com.google.auto.service.AutoService;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.netty.handler.MessageHandler;

import java.util.ServiceLoader;

@AutoService(NetServiceProvider.class)
public class NettyNetServiceProvider implements NetServiceProvider {

    public static final NettyNetService NET_SERVICE_INSTANCE = new NettyNetService();

    static {
        ServiceLoader.load(MessageHandler.Provider.class).iterator().forEachRemaining(provider -> {
        });
    }

    @Override
    public NetService get() {
        return NET_SERVICE_INSTANCE;
    }

    public NetService newService() {
        return new NettyNetService();
    }

}
