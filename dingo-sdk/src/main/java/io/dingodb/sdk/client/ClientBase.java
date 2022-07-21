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

package io.dingodb.sdk.client;

import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

@Slf4j
public class ClientBase {
    @Getter
    private NetService netService;
    @Getter
    private Location currentLocation;
    @Getter
    private CoordinatorConnector coordinatorConnector;
    private String configPath;
    private String coordinatorExchangeSvrList;
    private String currentHost;
    private Integer currentPort;

    public ClientBase(String configPath) {
        this.configPath = configPath;
    }

    public ClientBase(String coordinatorExchangeSvrList, String currentHost, Integer currentPort) {
        this.coordinatorExchangeSvrList = coordinatorExchangeSvrList;
        this.currentHost = currentHost;
        this.currentPort = currentPort;
    }

    public void initConnection() throws Exception {
        String connectionMode = "Property Files";
        try {
            if (this.configPath != null && !this.configPath.trim().isEmpty()) {
                // configuration mode
                DingoConfiguration.parse(this.configPath);
                this.currentHost = DingoConfiguration.host();
                this.currentPort = DingoConfiguration.port();

                this.currentLocation = new Location(currentHost, currentPort);
                this.coordinatorConnector = CoordinatorConnector.defaultConnector();
            } else {
                // connection string mode
                connectionMode = "Connection String";
                this.currentLocation = new Location(currentHost, currentPort);
                List<String> servers = Arrays.asList(coordinatorExchangeSvrList.split(","));

                List<Location> addrList = servers.stream()
                    .map(s -> s.split(":"))
                    .map(ss -> new Location(ss[0], Integer.parseInt(ss[1])))
                    .collect(Collectors.toList());
                this.coordinatorConnector = new CoordinatorConnector(addrList);
            }
            this.netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
        } catch (Exception ex) {
            log.error("Failed to initialize connection: connection mode:{}", connectionMode, ex.toString(), ex);
            throw ex;
        }
    }
}
