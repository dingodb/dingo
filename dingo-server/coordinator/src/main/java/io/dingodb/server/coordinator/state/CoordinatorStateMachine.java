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

package io.dingodb.server.coordinator.state;

import io.dingodb.common.concurrent.Executors;
import io.dingodb.meta.MetaService;
import io.dingodb.mpu.core.Core;
import io.dingodb.mpu.core.CoreListener;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.coordinator.api.CoordinatorServerApi;
import io.dingodb.server.coordinator.api.ScheduleApi;
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
import io.dingodb.server.coordinator.meta.adaptor.impl.BaseAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.BaseStatsAdaptor;
import io.dingodb.server.coordinator.metric.PartMetricCollector;
import io.dingodb.server.coordinator.schedule.ClusterScheduler;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.Tags;
import io.prometheus.client.exporter.HTTPServer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static io.dingodb.server.coordinator.meta.service.DingoMetaService.ROOT;

@Slf4j
public class CoordinatorStateMachine implements CoreListener {

    public static void init(Core core) {
        MetaStore metaStore = new MetaStore(core);
        CoordinatorStateMachine stateMachine = new CoordinatorStateMachine(core, metaStore);
    }

    private final Core core;

    private final MetaStore metaStore;

    private CoordinatorServerApi serverApi;
    private ScheduleApi scheduleApi;
    private final Set<Channel> leaderListener = new CopyOnWriteArraySet<>();

    private CoordinatorStateMachine(Core core, MetaStore metaStore) {
        this.core = core;
        this.metaStore = metaStore;
        NetService.getDefault().registerTagMessageListener(Tags.LISTEN_SERVICE_LEADER, this::onMessage);
        core.registerListener(this);
    }

    public boolean isPrimary() {
        return core.isPrimary();
    }

    @Override
    public void primary(final long clock) {
        log.info("On primary start: clock={}.", clock);
        if (serverApi == null) {
            this.serverApi = new CoordinatorServerApi(core);
        }
        if (scheduleApi == null) {
            scheduleApi = new ScheduleApi();
        }
        leaderListener.forEach(channel -> Executors.submit("primary-notify", () -> {
            log.info("Send primary message to [{}].", channel.remoteLocation().url());
            channel.send(Message.EMPTY);
        }));
        Executors.submit("on-primary", () -> {
            ServiceLoader.load(BaseAdaptor.Creator.class).iterator()
                .forEachRemaining(creator -> creator.create(metaStore));
            ServiceLoader.load(BaseStatsAdaptor.Creator.class).iterator()
                .forEachRemaining(creator -> creator.create(metaStore));
            ClusterScheduler.instance().init();
            MetaService.root();
            try {
                HTTPServer httpServer = new HTTPServer(CoordinatorConfiguration.monitorPort());
                new PartMetricCollector().register();
            } catch (IOException e) {
                log.error("http server error", e);
            }
        });
    }

    @Override
    public void back(long clock) {
        log.info("Primary back on {}", clock);
    }

    @Override
    public void losePrimary(long clock) {
        log.info("Lose primary on {}", clock);
    }

    @Override
    public void mirror(long clock) {
        log.info("On mirror start, clock: {}.", clock);
        if (serverApi == null) {
            this.serverApi = new CoordinatorServerApi(core);
        }
    }

    private void onMessage(Message message, Channel channel) {
        log.info("New leader listener channel, remote: [{}]", channel.remoteLocation());
        leaderListener.add(channel);
        channel.send(Message.EMPTY);
        if (isPrimary()) {
            channel.send(Message.EMPTY);
        }
        channel.setCloseListener(leaderListener::remove);
    }

}
