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

import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
@ToString
public class Configuration {

    private static final Configuration INSTANCE;

    static {
        try {
            DingoConfiguration.instance().setServer(Configuration.class);
            INSTANCE = DingoConfiguration.instance().getServer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Configuration instance() {
        return INSTANCE;
    }

    private Configuration() {
    }

    private String dataPath;
    private String dbRocksOptionsFile;
    private String logRocksOptionsFile;

    private String servers;
    private ScheduleConfiguration schedule = new ScheduleConfiguration();
    private Integer monitorPort = 9088;

    private boolean mem = false;

    public static String dataPath() {
        return INSTANCE.dataPath;
    }

    public static ScheduleConfiguration schedule() {
        return INSTANCE.schedule;
    }

    public static Integer monitorPort() {
        return INSTANCE.monitorPort;
    }

    public static boolean isMem() {
        return INSTANCE.mem;
    }

    public static List<Location> servers() {
        return Arrays.stream(INSTANCE.servers.split(","))
            .map(s -> s.split(":"))
            .map(__ -> new Location(__[0], Integer.parseInt(__[1])))
            .collect(Collectors.toList());
    }
}
