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

package io.dingodb.server.coordinator.config;

import com.alipay.sofa.jraft.entity.PeerId;
import io.dingodb.common.config.DingoConfiguration;
import lombok.experimental.Delegate;

import java.util.List;
import java.util.stream.Collectors;

public class CoordinatorConfiguration {

    public static final CoordinatorConfiguration INSTANCE = new CoordinatorConfiguration();

    public static final String COORDINATOR_SERVER_PORT = "coordinator.server.port";

    public static final String COORDINATOR_SERVERS_KEY = "coordinator.raft.servers";
    public static final String COORDINATOR_RAFT_GROUP_ID = "coordinator.raft.group.id";
    public static final String COORDINATOR_RAFT_SERVER_PORT = "coordinator.raft.port";

    public static final String DEFAULT_COORDINATOR_RAFT_ID = "COORDINATOR_RAFT";

    public static final String AUTO_SCHEDULE = "coordinator.schedule.auto";

    @Delegate private final DingoConfiguration dingoConfiguration = DingoConfiguration.INSTANCE;

    private CoordinatorConfiguration() {
    }

    public static CoordinatorConfiguration instance() {
        return INSTANCE;
    }

    public String coordinatorRaftId() {
        return getString(COORDINATOR_RAFT_GROUP_ID, DEFAULT_COORDINATOR_RAFT_ID);
    }

    public List<PeerId> coordinatorServers() {
        List<String> servers = getList(COORDINATOR_SERVERS_KEY);
        return servers.stream()
               .map(server -> server.split(":"))
               .map(hostIp -> new PeerId(hostIp[0], Integer.parseInt(hostIp[1])))
               .collect(Collectors.toList());
    }

    public int coordinatorRaftServerPort() {
        return getInt(COORDINATOR_RAFT_SERVER_PORT);
    }

    public boolean autoSchedule() {
        return getBool(AUTO_SCHEDULE, false);
    }

    public CoordinatorConfiguration autoSchedule(boolean autoSchedule) {
        setBool(AUTO_SCHEDULE, autoSchedule);
        return this;
    }

    public int port() {
        return getInt(COORDINATOR_SERVER_PORT, instancePort());
    }
}
