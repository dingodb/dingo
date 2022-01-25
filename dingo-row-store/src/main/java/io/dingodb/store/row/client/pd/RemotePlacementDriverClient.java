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

package io.dingodb.store.row.client.pd;

import io.dingodb.raft.RouteTable;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.store.row.errors.RouteTableException;
import io.dingodb.store.row.metadata.Cluster;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.store.row.metadata.Store;
import io.dingodb.store.row.options.PlacementDriverOptions;
import io.dingodb.store.row.options.RegionEngineOptions;
import io.dingodb.store.row.options.StoreEngineOptions;
import io.dingodb.store.row.util.Lists;
import io.dingodb.store.row.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class RemotePlacementDriverClient extends AbstractPlacementDriverClient {
    private static final Logger LOG = LoggerFactory.getLogger(RemotePlacementDriverClient.class);

    private String pdGroupId;
    private MetadataRpcClient metadataRpcClient;

    private boolean started;

    public RemotePlacementDriverClient(long clusterId, String clusterName) {
        super(clusterId, clusterName);
    }

    @Override
    public synchronized boolean init(final PlacementDriverOptions opts) {
        if (this.started) {
            LOG.info("[RemotePlacementDriverClient] already started.");
            return true;
        }
        super.init(opts);
        this.pdGroupId = opts.getPdGroupId();
        if (Strings.isBlank(this.pdGroupId)) {
            throw new IllegalArgumentException("opts.pdGroup id must not be blank");
        }
        final String initialPdServers = opts.getInitialPdServerList();
        if (Strings.isBlank(initialPdServers)) {
            throw new IllegalArgumentException("opts.initialPdServerList must not be blank");
        }
        RouteTable.getInstance().updateConfiguration(this.pdGroupId, initialPdServers);
        this.metadataRpcClient = new MetadataRpcClient(super.pdRpcService, 3);
        refreshRouteTable();
        LOG.info("[RemotePlacementDriverClient] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        super.shutdown();
        LOG.info("[RemotePlacementDriverClient] shutdown successfully.");
    }

    @Override
    protected void refreshRouteTable() {
        final Cluster cluster = this.metadataRpcClient.getClusterInfo(this.clusterId);
        if (cluster == null) {
            LOG.warn("Cluster info is empty: {}.", this.clusterId);
            return;
        }
        final List<Store> stores = cluster.getStores();
        if (stores == null || stores.isEmpty()) {
            LOG.error("Stores info is empty: {}.", this.clusterId);
            return;
        }
        for (final Store store : stores) {
            final List<Region> regions = store.getRegions();
            if (regions == null || regions.isEmpty()) {
                LOG.error("Regions info is empty: {} - {}.", this.clusterId, store.getId());
                continue;
            }
            for (final Region region : regions) {
                super.regionRouteTable.addOrUpdateRegion(region);
            }
        }
    }

    @Override
    public Store getStoreMetadata(final StoreEngineOptions opts) {
        final Endpoint selfEndpoint = opts.getServerAddress();
        // remote conf is the preferred
        final Store remoteStore = this.metadataRpcClient.getStoreInfo(this.clusterId, selfEndpoint);
        if (!remoteStore.isEmpty()) {
            final List<Region> regions = remoteStore.getRegions();
            for (final Region region : regions) {
                super.regionRouteTable.addOrUpdateRegion(region);
            }
            return remoteStore;
        }
        // todo remove local conf
        final Store localStore = new Store();
        final List<RegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        final List<Region> regionList = Lists.newArrayListWithCapacity(rOptsList.size());
        localStore.setId(remoteStore.getId());
        localStore.setEndpoint(selfEndpoint);
        for (final RegionEngineOptions rOpts : rOptsList) {
            regionList.add(getLocalRegionMetadata(rOpts));
        }
        localStore.setRegions(regionList);
        this.metadataRpcClient.updateStoreInfo(this.clusterId, localStore);
        return localStore;
    }

    @Override
    public Endpoint getPdLeader(final boolean forceRefresh, final long timeoutMillis) {
        PeerId leader = getLeaderForRaftGroupId(this.pdGroupId, forceRefresh, timeoutMillis);
        if (leader == null && !forceRefresh) {
            leader = getLeaderForRaftGroupId(this.pdGroupId, true, timeoutMillis);
        }
        if (leader == null) {
            throw new RouteTableException("no placement driver leader in group: " + this.pdGroupId);
        }
        return new Endpoint(leader.getIp(), leader.getPort());
    }

    public MetadataRpcClient getMetadataRpcClient() {
        return metadataRpcClient;
    }
}
