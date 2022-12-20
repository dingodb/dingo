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

package io.dingodb.server.client.executor.service;

import io.dingodb.common.Location;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.ExecutorApi;
import io.dingodb.verify.service.ExecutorService;
import lombok.experimental.Delegate;

public class ExecutorServiceClient implements ExecutorService {

    @Delegate
    public ExecutorApi executor;

    public ExecutorServiceClient(ApiRegistry apiRegistry,
                                 String leaderAddress) {
        String hostName = leaderAddress.split(":")[0];
        String port = leaderAddress.split(":")[1];
        executor = apiRegistry.proxy(
            ExecutorApi.class,
            () -> new Location(hostName, Integer.valueOf(port)));
    }
}
