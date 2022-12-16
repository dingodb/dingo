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

import io.dingodb.common.Location;
import io.dingodb.meta.SysInfoService;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.SysInfoServiceApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SysInfoServiceClient implements SysInfoService {

    @Delegate
    private SysInfoServiceApi sysInfoServiceApi;

    private final CoordinatorConnector connector;

    public SysInfoServiceClient() {
        this(CoordinatorConnector.getDefault());
    }

    public SysInfoServiceClient(CoordinatorConnector connector) {
        this.sysInfoServiceApi = ApiRegistry.getDefault().proxy(SysInfoServiceApi.class,
            NetService.getDefault().newChannel(connector.get()));
        this.connector = connector;
    }

    @Override
    public void flushPrivileges() {
        log.info("flush privileges");
        NetService netService = NetService.getDefault();
        Channel channel = netService.newChannel(connector.get());
        channel.send(new Message("LISTEN_RELOAD_PRIVILEGES", "flush privileges".getBytes()));
    }
}
