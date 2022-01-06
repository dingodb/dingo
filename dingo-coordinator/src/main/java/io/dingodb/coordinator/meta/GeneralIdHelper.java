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

package io.dingodb.coordinator.meta;

import com.alipay.sofa.jraft.util.Endpoint;
import io.dingodb.coordinator.GeneralId;

public class GeneralIdHelper {

    public static final String DEFAULT_REGION_NAME = "region";
    public static final String DEFAULT_EXECUTOR_NAME = "executorView@";
    public static final String DEFAULT_REGION_VIEW_NAME = "region";

    private GeneralIdHelper() {
    }

    public static GeneralId region(final long regionId) {
        return GeneralId.appOf(regionId, DEFAULT_REGION_NAME);
    }

    public static GeneralId region(final String regionId) {
        return GeneralId.appOf(Long.parseLong(regionId), DEFAULT_REGION_NAME);
    }

    public static GeneralId regionView(final long regionId) {
        return GeneralId.appViewOf(regionId, DEFAULT_REGION_VIEW_NAME);
    }

    public static GeneralId store(final Endpoint endpoint) {
        return GeneralId.resourceViewOf(0L, storeName(endpoint));
    }

    public static GeneralId store(final long storeId, final Endpoint endpoint) {
        return GeneralId.resourceViewOf(storeId, storeName(endpoint));
    }

    public static String storeName(final  Endpoint endpoint) {
        return DEFAULT_EXECUTOR_NAME + endpoint.toString();
    }

}
