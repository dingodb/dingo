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

package io.dingodb.executor.service;

import io.dingodb.helix.service.AbstractFollowerService;
import io.dingodb.helix.service.AbstractLeaderService;
import io.dingodb.helix.service.AbstractOfflineService;
import io.dingodb.helix.service.LeaderFollowerServiceProvider;
import io.dingodb.helix.service.StateServiceContext;
import io.dingodb.net.NetService;
import io.dingodb.store.StoreInstance;

public class ExecutorLeaderFollowerServiceProvider implements LeaderFollowerServiceProvider {

    private final NetService netService;
    private final StoreInstance storeInstance;

    public ExecutorLeaderFollowerServiceProvider(NetService netService, StoreInstance storeInstance) {
        this.netService = netService;
        this.storeInstance = storeInstance;
    }

    @Override
    public AbstractLeaderService leaderService(StateServiceContext context) {
        return new ExecutorLeaderService(context, netService, storeInstance);
    }

    @Override
    public AbstractFollowerService replicaService(StateServiceContext context) {
        return new ExecutorFollowerService(context, netService, storeInstance);
    }

    @Override
    public AbstractOfflineService offlineService(StateServiceContext context) {
        return new ExecutorOfflineService(context, netService, storeInstance);
    }
}
