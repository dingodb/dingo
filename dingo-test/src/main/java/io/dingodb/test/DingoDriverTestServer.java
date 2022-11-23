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

package io.dingodb.test;

import io.dingodb.exec.Services;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.driver.DriverProxyServer;

import java.util.ServiceLoader;

public final class DingoDriverTestServer {

    private static final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private static final int port = 8765;

    private DingoDriverTestServer() {
    }

    public static void main(String[] args) throws Exception {
        Services.initNetService();
        netService.listenPort(port);
        new DriverProxyServer().start();
    }
}
