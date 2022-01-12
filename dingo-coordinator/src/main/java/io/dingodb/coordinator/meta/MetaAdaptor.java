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
import io.dingodb.store.row.metadata.Cluster;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.store.row.metadata.RegionStats;
import io.dingodb.store.row.metadata.Store;
import io.dingodb.store.row.metadata.StoreStats;

import java.math.BigDecimal;
import java.util.Map;

public interface MetaAdaptor {
    Cluster cluster();

    Map<String, Endpoint> storeLocation();

    String storeId(Endpoint endpoint);

    Store storeInfo(Endpoint endpoint);

    Store storeInfo(GeneralId id);

    StoreStats storeStats(GeneralId id);

    BigDecimal storeScore(GeneralId id);

    void saveStore(Store store);

    void saveRegionHeartbeat(Region region, RegionStats regionStats);

    void saveStoreStats(StoreStats storeStats);

    String newRegionId();

}
