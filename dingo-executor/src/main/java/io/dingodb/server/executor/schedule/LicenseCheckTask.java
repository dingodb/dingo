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

package io.dingodb.server.executor.schedule;

import io.dingodb.cluster.ClusterService;
import io.dingodb.common.log.LogUtils;
import io.dingodb.license.LicenseService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.VersionService;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LicenseCheckTask implements Runnable {

    private static ClusterService clusterService;

    static {
        clusterService = ClusterService.getDefault();
    }

    @Override
    public void run() {
        String servers = Configuration.coordinators();
        LicenseService licenseService = LicenseService.getDefault();
        if (licenseService != null) {
            long timestamp = TsoService.getDefault().timestamp();

            int locations = clusterService.getLocations();
            int storeMap = clusterService.getStoreMap();

            boolean check = licenseService.check(timestamp, storeMap, locations, servers);
            LogUtils.info(log, "license check: {}", check);
            if (!check) {
                clusterService.configCoordinator(true, "License check failed");
            } else {
                clusterService.configCoordinator(false, "");
            }
        }
    }
}
