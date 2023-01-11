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

package io.dingodb.server.coordinator;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.core.Sidebar;
import io.dingodb.server.coordinator.api.CoordinatorServerApi;
import io.dingodb.server.coordinator.api.MetaServiceApi;
import io.dingodb.server.coordinator.api.ScheduleApi;
import io.dingodb.server.coordinator.api.UserServiceApi;
import io.dingodb.server.coordinator.config.Configuration;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.store.MetaStore;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.SERVICE_IDENTIFIER;

@Slf4j
public class CoordinatorSidebar extends Sidebar {

    public static CoordinatorSidebar INSTANCE;

    static {
        List<CoreMeta> metas = new ArrayList<>();
        List<Location> locations = Configuration.servers();
        for (int i = 0; i < locations.size(); i++) {
            Location location = locations.get(i);
            metas.add(new CoreMeta(
                CommonId.prefix(ID_TYPE.service, SERVICE_IDENTIFIER.coordinator, 1, i + 1),
                CommonId.prefix(ID_TYPE.service, SERVICE_IDENTIFIER.coordinator, 1, 1),
                location
            ));
        }
        INSTANCE = new CoordinatorSidebar(metas.remove(locations.indexOf(DingoConfiguration.location())), metas);
    }

    @Getter
    private final MetaStore metaStore;

    @Getter
    private CoordinatorServerApi serverApi;
    private ScheduleApi scheduleApi;

    private UserServiceApi userServiceApi;

    private CoordinatorSidebar(CoreMeta meta, List<CoreMeta> mirrors) {
        super(meta, mirrors, MetaStore.createStorage(meta.label), MetaAdaptorRegistry.getAdaptorIds());
        this.metaStore = new MetaStore(this);
        start(false);
    }

    @Override
    public void primary(final long clock) {
        super.primary(clock);
        log.info("On primary start: clock={}.", clock);
        if (serverApi == null) {
            this.serverApi = new CoordinatorServerApi();
        }
        if (scheduleApi == null) {
            scheduleApi = new ScheduleApi();
        }
        if (userServiceApi == null) {
            userServiceApi = new UserServiceApi();
        }

        MetaServiceApi instance = MetaServiceApi.INSTANCE;
        Executors.submit("on-primary", () -> {
            MetaAdaptorRegistry.reloadAllAdaptor();

            // Add permissions to the root user
            userServiceApi.saveRootPrivilege("root", "%");
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
            this.serverApi = new CoordinatorServerApi();
        }
    }

}
