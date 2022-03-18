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

package io.dingodb.server.client.meta.service;

import io.dingodb.common.config.DingoOptions;
import io.dingodb.meta.Location;
import io.dingodb.meta.MetaService;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.protocol.proto.TableEntry;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

@Slf4j
public class MetaServiceClient implements MetaService {

    public static final Location CURRENT_LOCATION = new Location(
        DingoOptions.instance().getIp(),
        DingoOptions.instance().getExchange().getPort(),
        ""
    );

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private final Map<String, TableEntry> tableEntries = new ConcurrentHashMap<>();
    private final CoordinatorConnector connector;

    @Delegate
    private final MetaServiceApi metaServiceApi;

    public MetaServiceClient(CoordinatorConnector connector) {
        this.connector = connector;
        //Channel tableListenerChannel = connector.newChannel();
        //tableListenerChannel.registerMessageListener((msg, ch) -> {
        //    ch.registerMessageListener(this::onTableMessage);
        //    ch.send(MetaServiceCode.LISTENER_TABLE.message());
        //    ch.send(MetaServiceCode.REFRESH_TABLES.message());
        //});
        //tableListenerChannel.send(BaseCode.PING.message(META_SERVICE));
        metaServiceApi = netService.apiRegistry().proxy(MetaServiceApi.class, connector);
    }

    @Override
    public String getName() {
        return "DINGO";
    }

    @Override
    public void init(@Nullable Map<String, Object> props) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Location currentLocation() {
        return CURRENT_LOCATION;
    }

}
