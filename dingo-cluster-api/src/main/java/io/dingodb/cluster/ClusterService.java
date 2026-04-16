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

package io.dingodb.cluster;


import io.dingodb.common.CommonId;
import io.dingodb.common.Executor;
import io.dingodb.common.Location;

import java.util.Collections;
import java.util.List;

public interface ClusterService {

    static ClusterService getDefault() {
        return ClusterServiceProvider.getDefault().get();
    }

    List<Location> getComputingLocations();

    CommonId getServerId(Location location);

    Location getLocation(CommonId serverId);

    List<Executor> getExecutors();

    int getStoreMap();

    int getLocations();

    void configCoordinator(boolean isReadOnly, String reason);

    default List<Object[]> getStoreNodes() {
        return Collections.emptyList();
    }

    default List<Object[]> getCoordinatorNodes() {
        return Collections.emptyList();
    }

    default int getRegionCount() {
        return 0;
    }

    default long getGcSafePoint() {
        return 0;
    }

    default List<Object[]> getJobList(Long jobId, Integer archiveLimit,
                                      boolean includeArchive, Long archiveStartId) {
        return Collections.emptyList();
    }
}
