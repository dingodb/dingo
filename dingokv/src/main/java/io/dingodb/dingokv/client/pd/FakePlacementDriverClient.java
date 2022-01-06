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

package io.dingodb.dingokv.client.pd;

import com.alipay.sofa.jraft.util.Endpoint;
import io.dingodb.dingokv.metadata.Region;
import io.dingodb.dingokv.metadata.Store;
import io.dingodb.dingokv.options.PlacementDriverOptions;
import io.dingodb.dingokv.options.RegionEngineOptions;
import io.dingodb.dingokv.options.StoreEngineOptions;
import io.dingodb.dingokv.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class FakePlacementDriverClient extends AbstractPlacementDriverClient {

    private static final Logger LOG = LoggerFactory.getLogger(FakePlacementDriverClient.class);

    private boolean started;

    public FakePlacementDriverClient(long clusterId, String clusterName) {
        super(clusterId, clusterName);
    }

    @Override
    public synchronized boolean init(final PlacementDriverOptions opts) {
        if (this.started) {
            LOG.info("[FakePlacementDriverClient] already started.");
            return true;
        }
        super.init(opts);
        LOG.info("[FakePlacementDriverClient] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        super.shutdown();
        LOG.info("[FakePlacementDriverClient] shutdown successfully.");
    }

    @Override
    protected void refreshRouteTable() {
        // NO-OP
    }

    @Override
    public Store getStoreMetadata(final StoreEngineOptions opts) {
        final Store store = new Store();
        final List<RegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        final List<Region> regionList = Lists.newArrayListWithCapacity(rOptsList.size());
        store.setId("-1");
        store.setEndpoint(opts.getServerAddress());
        for (final RegionEngineOptions rOpts : rOptsList) {
            regionList.add(getLocalRegionMetadata(rOpts));
        }
        store.setRegions(regionList);
        return store;
    }

    @Override
    public Endpoint getPdLeader(final boolean forceRefresh, final long timeoutMillis) {
        throw new UnsupportedOperationException();
    }
}
