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

package io.dingodb.server.client.user.service;

import io.dingodb.common.CommonId;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.UserServiceApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.client.meta.service.MetaServiceClientProvider;
import io.dingodb.verify.service.UserService;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserServiceClient implements UserService {

    @Delegate
    private UserServiceApi userServiceApi;

    private final CoordinatorConnector connector;

    public UserServiceClient() {
        this(CoordinatorConnector.getDefault());
    }

    public UserServiceClient(CoordinatorConnector connector) {
        this.userServiceApi = ApiRegistry.getDefault().proxy(UserServiceApi.class, connector);
        this.connector = connector;
    }

    @Override
    public CommonId getSchemaId(String schema) {
        return MetaServiceClientProvider.META_SERVICE_CLIENT.getSubMetaService(schema).id();
    }

    @Override
    public CommonId getTableId(CommonId schemaId, String table) {
        return MetaServiceClientProvider.META_SERVICE_CLIENT.getSubMetaService(schemaId).getTableId(table);
    }

    @Override
    public void flushPrivileges() {
        log.info("flush privileges");
        NetService netService = NetService.getDefault();
        Channel channel = netService.newChannel(connector.get());
        channel.send(new Message("LISTEN_RELOAD_PRIVILEGES", "flush privileges".getBytes()));
    }
}
