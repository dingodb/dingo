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

package io.dingodb.server.executor.common;

import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.cluster.Executor;
import io.dingodb.sdk.common.cluster.ExecutorUser;
import io.dingodb.sdk.common.cluster.InternalExecutorUser;
import io.dingodb.server.executor.Configuration;

public class ClusterHeartbeatExecutor implements Executor {
    @Override
    public Location serverLocation() {
        return new Location(DingoConfiguration.host(), DingoConfiguration.port());
    }

    @Override
    public ExecutorUser executorUser() {
        return new InternalExecutorUser(Configuration.user(), Configuration.keyring());
    }

    @Override
    public String resourceTag() {
        return Configuration.resourceTag();
    }
}
