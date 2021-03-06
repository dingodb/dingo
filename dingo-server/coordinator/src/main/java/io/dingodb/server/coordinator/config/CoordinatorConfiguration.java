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

import io.dingodb.common.config.DingoConfiguration;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class CoordinatorConfiguration {

    private static final CoordinatorConfiguration INSTANCE;

    static {
        try {
            DingoConfiguration.instance().setServer(CoordinatorConfiguration.class);
            INSTANCE = DingoConfiguration.instance().getServer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static CoordinatorConfiguration instance() {
        return INSTANCE;
    }

    private CoordinatorConfiguration() {
    }

    private String dataPath;
    private String dbRocksOptionsFile;
    private String logRocksOptionsFile;

    private RaftConfiguration raft;
    private ScheduleConfiguration schedule = new ScheduleConfiguration();
    private Integer monitorPort = 9088;

    public static String dataPath() {
        return INSTANCE.dataPath;
    }

    public static RaftConfiguration raft() {
        return INSTANCE.raft;
    }

    public static ScheduleConfiguration schedule() {
        return INSTANCE.schedule;
    }

    public static Integer monitorPort() {
        return INSTANCE.monitorPort;
    }
}
