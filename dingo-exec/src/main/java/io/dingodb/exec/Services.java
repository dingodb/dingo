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

package io.dingodb.exec;

import io.dingodb.common.Location;
import io.dingodb.common.error.DingoException;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.channel.EndpointManager;
import io.dingodb.exec.impl.JobManagerImpl;
import io.dingodb.net.Channel;
import io.dingodb.net.NetError;
import io.dingodb.net.NetService;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.api.StoreServiceProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public final class Services {
    public static final StoreService KV_STORE = Optional.ofNullable(ServiceProviders.KV_STORE_PROVIDER.provider())
        .map(StoreServiceProvider::get).orNull();
    public static final StoreService LOCAL_STORE = Optional.mapOrNull(
        StoreServiceProvider.get("local"), StoreServiceProvider::get
    );
    public static final NetService NET = Objects.requireNonNull(
        ServiceProviders.NET_PROVIDER.provider(),
        "No channel service provider was found."
    ).get();
    public static final String CTRL_TAG = "DINGO_CTRL";

    private Services() {
    }

    public static void initNetService() {
        initControlMsgService();
        NET.registerTagMessageListener(JobManagerImpl.TASK_TAG, (message, channel) ->
            JobManagerImpl.INSTANCE.processMessage(message));
    }

    public static void initControlMsgService() {
        NET.registerTagMessageListener(CTRL_TAG, (message, channel) -> {
            EndpointManager.INSTANCE.onControlMessage(message);
        });
    }

    public static Channel openNewChannel(String host, int port) {
        int count = 0;
        while (count < 3) {
            try {
                return Services.NET.newChannel(new Location(host, port));
            } catch (DingoException e) {
                if (e.getCategory() == NetError.OPEN_CHANNEL_TIME_OUT
                    || e.getCategory() == NetError.OPEN_CONNECTION_TIME_OUT
                ) {
                    ++count;
                    continue;
                }
                throw new RuntimeException(e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        throw new IllegalStateException(
            "Tried to open new channel to \"" + host + ":" + port + "\" 3 times, but all failed."
        );
    }

    public static Channel openNewSysChannel(String host, int port) {
        return openNewChannel(host, port);
    }
}
