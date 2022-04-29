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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.cluster.ClusterService;
import io.dingodb.common.error.DingoException;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.channel.EndpointManager;
import io.dingodb.exec.impl.TaskImpl;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.net.Channel;
import io.dingodb.net.NetAddress;
import io.dingodb.net.NetError;
import io.dingodb.net.NetService;
import io.dingodb.net.SimpleTag;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.api.StoreServiceProvider;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public final class Services {
    public static final StoreService KV_STORE = Optional.ofNullable(ServiceProviders.KV_STORE_PROVIDER.provider())
        .map(StoreServiceProvider::get).orNull();
    public static final MetaService META = Objects.requireNonNull(
        ServiceProviders.META_PROVIDER.provider(),
        "No meta service provider was found."
    ).get();
    public static final NetService NET = Objects.requireNonNull(
        ServiceProviders.NET_PROVIDER.provider(),
        "No channel service provider was found."
    ).get();
    public static final ClusterService CLUSTER = Objects.requireNonNull(
        ServiceProviders.CLUSTER_PROVIDER.provider(),
        "No cluster service provider was found."
    ).get();
    public static final Map<String, MetaService> metaServices = new HashMap<>();

    private static final ExecutorService executorService = Executors.newWorkStealingPool();

    static {
        initMetaServices();
    }

    private Services() {
    }

    public static void initMetaServices() {
        for (MetaServiceProvider provider : ServiceProviders.META_PROVIDER) {
            MetaService metaService = provider.get();
            String serviceName = metaService.getName();
            if (metaServices.containsKey(serviceName)) {
                throw new RuntimeException("Duplicate meta service name \"" + serviceName + "\" exists.");
            }
            metaServices.put(metaService.getName(), metaService);
        }
    }

    public static void initNetService() {
        initControlMsgService();
        NET.registerTagMessageListener(SimpleTag.TASK_TAG, (message, channel) -> {
            String taskStr = new String(message.toBytes(), StandardCharsets.UTF_8);
            if (log.isInfoEnabled()) {
                log.info("Received task: {}", taskStr);
            }
            try {
                Task task = TaskImpl.deserialize(taskStr);
                executorService.execute(() -> {
                    task.init();
                    task.run();
                });
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot deserialize received task.", e);
            }
        });
    }

    public static void initControlMsgService() {
        NET.registerTagMessageListener(SimpleTag.CTRL_TAG, (message, channel) -> {
            EndpointManager.INSTANCE.onControlMessage(message);
        });
    }

    public static Channel openNewChannel(String host, int port) {
        int count = 0;
        while (count < 3) {
            try {
                return Services.NET.newChannel(new NetAddress(host, port));
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
