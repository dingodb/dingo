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

package io.dingodb.driver;

import io.dingodb.driver.api.DriverProxyApi;
import io.dingodb.net.NetAddressProvider;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import lombok.Setter;
import lombok.experimental.Delegate;
import org.apache.calcite.avatica.remote.Service;

import java.util.ServiceLoader;

public class DingoServiceImpl implements Service {

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    @Setter
    private RpcMetadataResponse rpcMetadata;

    @Delegate
    private final DriverProxyApi proxyApi;

    public DingoServiceImpl(NetAddressProvider netAddressProvider) {
        proxyApi = netService.apiRegistry().proxy(DriverProxyApi.class, netAddressProvider);
    }
}
