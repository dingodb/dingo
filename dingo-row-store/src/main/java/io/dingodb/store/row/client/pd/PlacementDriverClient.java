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

import io.dingodb.raft.Lifecycle;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.store.row.client.RegionRouteTable;
import io.dingodb.store.row.metadata.Peer;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.store.row.metadata.Store;
import io.dingodb.store.row.options.PlacementDriverOptions;
import io.dingodb.store.row.options.StoreEngineOptions;
import io.dingodb.store.row.storage.CASEntry;
import io.dingodb.store.row.storage.KVEntry;

import java.util.List;
import java.util.Map;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface PlacementDriverClient extends Lifecycle<PlacementDriverOptions> {
    /**
     * Returns the cluster id.
     */
    long getClusterId();

    /**
     * Query the region by region id.
     */
    Region getRegionById(final String regionId);

    /**
     * Returns the region to which the key belongs.
     */
    Region findRegionByKey(final byte[] key, final boolean forceRefresh);

    /**
     * Returns the regions to which the keys belongs.
     */
    Map<Region, List<byte[]>> findRegionsByKeys(final List<byte[]> keys, final boolean forceRefresh);

    /**
     * Returns the regions to which the keys belongs.
     */
    Map<Region, List<KVEntry>> findRegionsByKvEntries(final List<KVEntry> kvEntries, final boolean forceRefresh);

    /**
     * Returns the regions to which the keys belongs.
     */
    Map<Region, List<CASEntry>> findRegionsByCASEntries(final List<CASEntry> casEntries, final boolean forceRefresh);

    /**
     * Returns the list of regions covered by startKey and endKey.
     */
    List<Region> findRegionsByKeyRange(final byte[] startKey, final byte[] endKey, final boolean forceRefresh);

    /**
     * Returns the startKey of next region.
     */
    byte[] findStartKeyOfNextRegion(final byte[] key, final boolean forceRefresh);

    /**
     * Returns the regionRouteTable instance.
     */
    RegionRouteTable getRegionRouteTable();

    /**
     * Returns the store metadata of the current instance's store.
     * Construct initial data based on the configuration file if
     * the data on {@link PlacementDriverClient} is empty.
     */
    Store getStoreMetadata(final StoreEngineOptions opts);

    /**
     * Get the specified region leader communication address.
     */
    Endpoint getLeader(final String regionId, final boolean forceRefresh, final long timeoutMillis);

    /**
     * Get the specified region random peer communication address,
     * format: [ip:port]
     */
    Endpoint getLuckyPeer(final String regionId, final boolean forceRefresh, final long timeoutMillis,
                          final Endpoint unExpect);

    /**
     * Refresh the routing information of the specified region
     */
    void refreshRouteConfiguration(final String regionId);

    /**
     * Transfer leader to specified peer.
     */
    boolean transferLeader(final String regionId, final Peer peer, final boolean refreshConf);

    /**
     * Join the specified region group.
     */
    boolean addReplica(final String regionId, final Peer peer, final boolean refreshConf);

    /**
     * Depart from the specified region group.
     */
    boolean removeReplica(final String regionId, final Peer peer, final boolean refreshConf);

    /**
     * Returns raft cluster prefix id.
     */
    String getClusterName();

    /**
     * Get the placement driver server's leader communication address.
     */
    Endpoint getPdLeader(final boolean forceRefresh, final long timeoutMillis);

    /**
     * Returns the pd rpc service client.
     */
    PlacementDriverRpcService getPdRpcService();
}
