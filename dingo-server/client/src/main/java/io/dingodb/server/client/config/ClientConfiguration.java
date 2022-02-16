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

package io.dingodb.server.client.config;

import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.net.NetAddress;
import lombok.experimental.Delegate;

import java.util.List;
import java.util.stream.Collectors;

@Deprecated
public class ClientConfiguration {

    public static final ClientConfiguration INSTANCE = new ClientConfiguration();

    public static final String COORDINATOR_SERVERS_KEY = "client.coordinator.servers";
    public static final String EXECUTOR_SERVERS_KEY = "client.executors.servers";

    @Delegate private final DingoConfiguration dingoConfiguration = DingoConfiguration.instance();

    public List<NetAddress> coordinatorServers() {
        List<String> servers = getList(COORDINATOR_SERVERS_KEY);
        return servers.stream()
            .map(s -> s.split(":"))
            .map(ss -> new NetAddress(ss[0], Integer.parseInt(ss[1])))
            .collect(Collectors.toList());
    }

    public List<NetAddress> executorServers() {
        List<String> servers = getList(EXECUTOR_SERVERS_KEY);
        return servers.stream()
            .map(s -> s.split(":"))
            .map(ss -> new NetAddress(ss[0], Integer.parseInt(ss[1])))
            .collect(Collectors.toList());
    }

}
