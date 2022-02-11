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

package io.dingodb.server.coordinator.meta;

import com.alipay.remoting.util.ConcurrentHashSet;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.store.row.metadata.RegionStats;
import io.dingodb.store.row.util.Lists;
import io.dingodb.store.row.util.Maps;
import io.dingodb.store.row.util.Pair;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class ClusterStatsManager {

    private static final ConcurrentMap<Long, ClusterStatsManager> clusterStatsManagerTable = Maps.newConcurrentMap();

    private final long clusterId;
    // Map<StoreId, Set<RegionId>>
    private final ConcurrentMap<String, Set<String>> leaderTable = Maps.newConcurrentMap();
    // Map<RegionId, Pair<Region, RegionStats>>
    private final ConcurrentMap<String, Pair<Region, RegionStats>>  regionStatsTable = Maps.newConcurrentMap();

    private ClusterStatsManager(long clusterId) {
        this.clusterId = clusterId;
    }

    public static ClusterStatsManager getInstance(final long clusterId) {
        ClusterStatsManager instance = clusterStatsManagerTable.get(clusterId);
        if (instance == null) {
            final ClusterStatsManager newInstance = new ClusterStatsManager(clusterId);
            instance = clusterStatsManagerTable.putIfAbsent(clusterId, newInstance);
            if (instance == null) {
                instance = newInstance;
            }
        }
        return instance;
    }

    public long getClusterId() {
        return clusterId;
    }

    public int regionSize() {
        return this.regionStatsTable.size();
    }

    public void addOrUpdateLeader(final String storeId, final String regionId) {
        Set<String> regionTable = this.leaderTable.get(storeId);
        if (regionTable == null) {
            final Set<String> newRegionTable = new ConcurrentHashSet<>();
            regionTable = this.leaderTable.putIfAbsent(storeId, newRegionTable);
            if (regionTable == null) {
                regionTable = newRegionTable;
            }
        }
        if (regionTable.add(regionId)) {
            for (final Map.Entry<String, Set<String>> entry : this.leaderTable.entrySet()) {
                if (storeId.equals(entry.getKey())) {
                    continue;
                }
                entry.getValue().remove(regionId);
            }
        }
    }

    // Looking for a model worker
    public Pair<Set<String /* storeId */>, Integer /* leaderCount */> findModelWorkerStores(final int above) {
        final Set<Map.Entry<String, Set<String>>> values = this.leaderTable.entrySet();
        if (values.isEmpty()) {
            return Pair.of(Collections.emptySet(), 0);
        }
        final Map.Entry<String, Set<String>> modelWorker = Collections.max(values, (o1, o2) -> {
            final int o1Val = o1.getValue() == null ? 0 : o1.getValue().size();
            final int o2Val = o2.getValue() == null ? 0 : o2.getValue().size();
            return Integer.compare(o1Val, o2Val);
        });
        final int maxLeaderCount = modelWorker.getValue().size();
        if (maxLeaderCount <= above) {
            return Pair.of(Collections.emptySet(), maxLeaderCount);
        }
        final Set<String> modelWorkerStoreIds = new HashSet<>();
        for (final Map.Entry<String, Set<String>> entry : values) {
            if (entry.getValue().size() >= maxLeaderCount) {
                modelWorkerStoreIds.add(entry.getKey());
            }
        }
        return Pair.of(modelWorkerStoreIds, maxLeaderCount);
    }

    // Investigate who is lazy
    public List<Pair<String /* storeId */, Integer /* leaderCount */>> findLazyWorkerStores(
        final Collection<String> storeCandidates
    ) {
        if (storeCandidates == null || storeCandidates.isEmpty()) {
            return Collections.emptyList();
        }
        final Set<Map.Entry<String, Set<String>>> values = this.leaderTable.entrySet();
        if (values.isEmpty()) {
            return Collections.emptyList();
        }
        final Map.Entry<String, Set<String>> lazyWorker = Collections.min(values, (o1, o2) -> {
            final int o1Val = o1.getValue() == null ? 0 : o1.getValue().size();
            final int o2Val = o2.getValue() == null ? 0 : o2.getValue().size();
            return Integer.compare(o1Val, o2Val);
        });
        final int minLeaderCount = lazyWorker.getValue().size();
        final List<Pair<String, Integer>> lazyCandidates = Lists.newArrayList();
        for (final String storeId : storeCandidates) {
            final Set<String> regionTable = this.leaderTable.get(storeId);
            int leaderCount = regionTable == null ? 0 : regionTable.size();
            if (leaderCount <= minLeaderCount) {
                lazyCandidates.add(Pair.of(storeId, leaderCount));
            }
        }
        return lazyCandidates;
    }

    public void addOrUpdateRegionStats(final List<Pair<Region, RegionStats>> regionStatsList) {
        for (final Pair<Region, RegionStats> p : regionStatsList) {
            this.regionStatsTable.put(p.getKey().getId(), p);
        }
    }

    public void addOrUpdateRegionStats(Region region, RegionStats regionStats) {
        this.regionStatsTable.put(region.getId(), Pair.of(region, regionStats));
    }

    public Pair<Region, RegionStats> findModelWorkerRegion() {
        if (this.regionStatsTable.isEmpty()) {
            return null;
        }
        return Collections.max(this.regionStatsTable.values(), (o1, o2) -> {
            final long o1Val = o1.getValue().getApproximateKeys();
            final long o2Val = o2.getValue().getApproximateKeys();
            return Long.compare(o1Val, o2Val);
        });
    }

    public static void invalidCache() {
        for (final ClusterStatsManager manager : clusterStatsManagerTable.values()) {
            manager.leaderTable.clear();
            manager.regionStatsTable.clear();
        }
        clusterStatsManagerTable.clear();
    }
}
