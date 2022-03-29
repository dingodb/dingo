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

package io.dingodb.server.driver;

import io.dingodb.driver.api.DriverProxyApi;
import io.dingodb.driver.server.ServerMetaFactory;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import lombok.experimental.Delegate;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;

import java.util.Collections;
import java.util.ServiceLoader;

public class DriverProxyService implements Service, DriverProxyApi {

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    @Delegate
    private final LocalService localService;

    public DriverProxyService() throws Exception {
        Class<Meta.Factory> factoryClass =
            (Class<Meta.Factory>) Class.forName(ServerMetaFactory.class.getCanonicalName());
        Meta.Factory factory = factoryClass.getConstructor().newInstance();
        Meta meta = factory.create(Collections.emptyList());
        this.localService = new LocalService(meta);
    }

    public void start() {
        netService.apiRegistry().register(DriverProxyApi.class, this);
    }

    public void stop() {
        netService.apiRegistry().register(DriverProxyApi.class, null);
    }
}
