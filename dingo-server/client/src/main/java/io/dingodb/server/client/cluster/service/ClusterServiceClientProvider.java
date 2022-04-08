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

package io.dingodb.server.client.cluster.service;

import com.google.auto.service.AutoService;
import io.dingodb.cluster.ClusterService;
import io.dingodb.cluster.ClusterServiceProvider;
import io.dingodb.common.config.DingoOptions;
import io.dingodb.net.NetAddress;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@AutoService(ClusterServiceProvider.class)
public class ClusterServiceClientProvider implements ClusterServiceProvider {

    private static final ClusterServiceClient CLUSTER_SERVICE_CLIENT;

    static {
        int times = DingoOptions.instance().getCliOptions().getMaxRetry();
        final String coordSrvList = DingoOptions.instance().getCoordOptions().getInitCoordExchangeSvrList();
        log.info("Cluster service provider, coordSrvList: {}", coordSrvList);

        List<NetAddress> addrList = Arrays.stream(coordSrvList.split(","))
            .map(s -> s.split(":"))
            .map(ss -> new NetAddress(ss[0], Integer.parseInt(ss[1])))
            .collect(Collectors.toList());

        CoordinatorConnector connector = new CoordinatorConnector(addrList);

        int sleep = 200;
        do {
            connector.refresh();
            try {
                Thread.sleep(sleep);
                sleep += sleep;
            } catch (InterruptedException e) {
                System.err.println("Wait coordinator connector ready interrupt.");
            }
        }
        while (!connector.verify() && times-- > 0);
        CLUSTER_SERVICE_CLIENT = new ClusterServiceClient(connector);
    }

    @Override
    public ClusterService get() {
        return CLUSTER_SERVICE_CLIENT;
    }
}
