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

package io.dingodb.web.config;

import io.dingodb.common.Location;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.api.MetaApi;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

@Component
public class ClientConfig {

    @Value("${server.coordinatorExchangeSvrList}")
    private String coordSrvList;

    @Bean
    public CoordinatorConnector coordinatorConnector(
        @Value("${server.coordinatorExchangeSvrList}") String coordSrvList
    ) {
        List<String> servers = Arrays.asList(coordSrvList.split(","));
        List<Location> addrList = servers.stream()
            .map(s -> s.split(":"))
            .map(ss -> new Location(ss[0], Integer.parseInt(ss[1])))
            .collect(Collectors.toList());
        return new CoordinatorConnector(addrList);
    }

    @Bean
    public MetaApi metaApi(CoordinatorConnector connector) {
        return ServiceLoader.load(NetServiceProvider.class).iterator().next().get()
            .apiRegistry().proxy(MetaApi.class, connector);
    }

    @Bean
    public MetaServiceApi metaServiceApi(CoordinatorConnector connector) {
        NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
        return netService.apiRegistry().proxy(MetaServiceApi.class, connector);
    }
}
