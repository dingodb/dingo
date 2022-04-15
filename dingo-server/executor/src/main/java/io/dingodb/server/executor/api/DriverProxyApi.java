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

package io.dingodb.server.executor.api;

import io.dingodb.net.NetService;
import io.dingodb.server.driver.DriverProxyService;
import io.dingodb.server.protocol.proto.DriverProxy;

public class DriverProxyApi implements io.dingodb.server.api.DriverProxyApi {

    private final DriverProxyService service = new DriverProxyService();

    public DriverProxyApi(NetService netService) {
        netService.apiRegistry().register(io.dingodb.server.api.DriverProxyApi.class, this);
    }

    @Override
    public void start(DriverProxy driverProxy) {
        service.start();
    }

    @Override
    public void stop(DriverProxy driverProxy) {
        service.stop();
    }

}
