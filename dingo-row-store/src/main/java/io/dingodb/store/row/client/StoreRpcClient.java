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

package io.dingodb.store.row.client;

import io.dingodb.net.NetAddress;
import io.dingodb.net.NetAddressProvider;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.store.row.rpc.CompareRegionApi;
import io.dingodb.store.row.rpc.ReportToLeaderApi;

import java.util.ServiceLoader;

public class StoreRpcClient implements NetAddressProvider  {
    private static final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private String ip;
    private int port;

    @Override
    public NetAddress get() {
        return new NetAddress(ip, port);
    }

    public ReportToLeaderApi getReportToLeadeApi(String leaderIp, int port) {
        this.ip = leaderIp;
        this.port = port;
        return netService.apiRegistry().proxy(ReportToLeaderApi.class, this);
    }

    public CompareRegionApi getCompareRegionApi(String ip, int port) {
        this.ip = ip;
        this.port = port;
        return netService.apiRegistry().proxy(CompareRegionApi.class, this);
    }
}
