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

package io.dingodb.calcite.executor;

import io.dingodb.cluster.ClusterService;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ShowCapacityExecutor extends QueryExecutor {

    private ClusterService clusterService;

    public ShowCapacityExecutor() {
        clusterService = ClusterService.getDefault();
    }

    @Override
    Iterator<Object[]> getIterator() {
        int executorCount = clusterService.getExecutors().size();
        int storeCount = clusterService.getStoreMap();
        int coordinatorCount = clusterService.getCoordinatorNodes().size();
        int locationCount = clusterService.getLocations();
        int regionCount = clusterService.getRegionCount();

        Runtime runtime = Runtime.getRuntime();
        long maxMemoryMB = runtime.maxMemory() / (1024 * 1024);
        long totalMemoryMB = runtime.totalMemory() / (1024 * 1024);
        long freeMemoryMB = runtime.freeMemory() / (1024 * 1024);
        long usedMemoryMB = totalMemoryMB - freeMemoryMB;
        int availableProcessors = runtime.availableProcessors();

        File root = new File("/");
        long totalDiskGB = root.getTotalSpace() / (1024 * 1024 * 1024);
        long freeDiskGB = root.getUsableSpace() / (1024 * 1024 * 1024);

        return Collections.singletonList(
            new Object[] {
                executorCount, storeCount, coordinatorCount,
                locationCount, regionCount,
                maxMemoryMB, usedMemoryMB, freeMemoryMB,
                availableProcessors, totalDiskGB, freeDiskGB
            }
        ).iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("executorCount");
        columns.add("storeCount");
        columns.add("coordinatorCount");
        columns.add("locationCount");
        columns.add("regionCount");
        columns.add("jvmMaxMemoryMB");
        columns.add("jvmUsedMemoryMB");
        columns.add("jvmFreeMemoryMB");
        columns.add("availableProcessors");
        columns.add("totalDiskGB");
        columns.add("freeDiskGB");
        return columns;
    }
}
