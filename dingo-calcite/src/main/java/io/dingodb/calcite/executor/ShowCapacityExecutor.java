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
import io.dingodb.common.Executor;
import io.dingodb.common.Location;
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.net.api.ApiRegistry;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class ShowCapacityExecutor extends QueryExecutor {

    public interface Api {
        @ApiDeclaration
        default long[] getResourceInfo() {
            return ShowCapacityExecutor.getLocalResourceInfo();
        }
    }

    public static final List<String> COLUMNS = Arrays.asList(
        "type", "id", "host", "port",
        "jvmMaxMemoryMB", "jvmUsedMemoryMB", "jvmFreeMemoryMB",
        "availableProcessors", "totalDiskGB", "freeDiskGB"
    );

    private static final long[] EMPTY_RESOURCE_INFO = new long[] {0L, 0L, 0L, 0L, 0L, 0L};

    public ShowCapacityExecutor() {
    }

    static long[] getLocalResourceInfo() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemoryMB = runtime.maxMemory() / (1024 * 1024);
        long totalMemoryMB = runtime.totalMemory() / (1024 * 1024);
        long freeMemoryMB = runtime.freeMemory() / (1024 * 1024);
        long usedMemoryMB = totalMemoryMB - freeMemoryMB;
        int availableProcessors = runtime.availableProcessors();

        File root = new File(System.getProperty("user.dir")).toPath().getRoot().toFile();
        long totalDiskGB = root.getTotalSpace() / (1024 * 1024 * 1024);
        long freeDiskGB = root.getUsableSpace() / (1024 * 1024 * 1024);

        return new long[] {
            maxMemoryMB, usedMemoryMB, freeMemoryMB,
            availableProcessors, totalDiskGB, freeDiskGB
        };
    }

    @Override
    Iterator<Object[]> getIterator() {
        List<Object[]> results = new ArrayList<>();
        List<Executor> executors = ClusterService.getDefault().getExecutors();

        Location local = DingoConfiguration.location();

        for (Executor executor : executors) {
            Location location = new Location(executor.getHost(), executor.getPort());
            try {
                long[] info;
                if (location.equals(local)) {
                    info = getLocalResourceInfo();
                } else {
                    Api proxy = ApiRegistry.getDefault().proxy(Api.class, location);
                    info = proxy.getResourceInfo();
                }
                results.add(new Object[] {
                    "Executor", executor.getId(),
                    location.getHost(), location.getPort(),
                    info[0], info[1], info[2],
                    info[3], info[4], info[5]
                });
            } catch (Exception e) {
                LogUtils.error(log, "Failed to get resource info from host: "
                    + location.getHost() + ", port: " + location.getPort() + ": " + e.getMessage(), e);
                results.add(new Object[] {
                    "Executor", executor.getId(),
                    location.getHost(), location.getPort(),
                    EMPTY_RESOURCE_INFO[0], EMPTY_RESOURCE_INFO[1], EMPTY_RESOURCE_INFO[2],
                    EMPTY_RESOURCE_INFO[3], EMPTY_RESOURCE_INFO[4], EMPTY_RESOURCE_INFO[5]
                });
            }
        }
        return results.iterator();
    }

    @Override
    public List<String> columns() {
        return COLUMNS;
    }
}
