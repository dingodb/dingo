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

package io.dingodb.coordinator.meta.impl;

import com.alipay.remoting.util.ConcurrentHashSet;
import com.alipay.sofa.jraft.util.Endpoint;
import io.dingodb.common.util.Optional;
import io.dingodb.coordinator.GeneralId;
import io.dingodb.coordinator.app.impl.RegionApp;
import io.dingodb.coordinator.app.impl.RegionView;
import io.dingodb.coordinator.meta.GeneralIdHelper;
import io.dingodb.coordinator.meta.MetaAdaptor;
import io.dingodb.coordinator.meta.MetaStore;
import io.dingodb.coordinator.resource.impl.ExecutorView;
import io.dingodb.store.row.metadata.Cluster;
import io.dingodb.store.row.metadata.Peer;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.store.row.metadata.RegionEpoch;
import io.dingodb.store.row.metadata.RegionStats;
import io.dingodb.store.row.metadata.Store;
import io.dingodb.store.row.metadata.StoreLabel;
import io.dingodb.store.row.metadata.StoreStats;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class MetaAdaptorImpl implements MetaAdaptor {

    private final MetaStore metaStore;

    private final Map<Endpoint, GeneralId> endpointStoreId = new ConcurrentHashMap<>();
    private final Map<GeneralId, Store> storeMap = new ConcurrentHashMap<>();
    private final Map<GeneralId, StoreStats> storeStatsMap = new ConcurrentHashMap<>();

    private final Map<GeneralId, Region> regionMap = new ConcurrentHashMap<>();
    private final Map<GeneralId, RegionStats> regionStatsMap = new ConcurrentHashMap<>();

    private final Map<GeneralId, GeneralId> regionLeaderMap = new ConcurrentHashMap<>();
    private final Map<GeneralId, Set<GeneralId>> storeRegion = new ConcurrentHashMap<>();
    private final Map<GeneralId, Set<GeneralId>> storeLeaderRegion = new ConcurrentHashMap<>();

    public MetaAdaptorImpl(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    @Override
    public Cluster cluster() {
        List<Store> stores = metaStore.namespaceView().resourceViews().values().stream()
            .filter(ExecutorView.class::isInstance)
            .map(ExecutorView.class::cast)
            .map(this::mapping)
            .collect(Collectors.toList());
        Cluster cluster = new Cluster(0, stores);
        log.info("Get cluster, cluster: {}", cluster);
        return cluster;
    }

    @Override
    public Map<String, Endpoint> storeLocation() {
        return storeMap.entrySet().stream().collect(Collectors.toMap(
            e -> e.getKey().toString(),
            e -> e.getValue().getEndpoint()
        ));
    }

    @Override
    public String storeId(Endpoint endpoint) {
        return storeIdForEndpoint(endpoint)
            .ifAbsentSet(() -> GeneralIdHelper.store(metaStore.newResourceSeq().join(), endpoint))
            .map(GeneralId::toString)
            .orNull();
    }

    @Override
    public Store storeInfo(GeneralId id) {
        return storeMap
            .computeIfAbsent(id, k -> mapping(metaStore.executorView(id)));
    }

    @Override
    public Store storeInfo(Endpoint endpoint) {
        return storeIdForEndpoint(endpoint)
            .map(storeMap::get)
            .ifAbsentSet(() -> Optional.ofNullable(metaStore.executorView(endpoint))
                .ifPresent(e -> endpointStoreId.put(endpoint, e.resourceId()))
                .map(e -> storeMap.computeIfAbsent(e.resourceId(), k -> mapping(e)))
                .orNull())
            .orNull();
    }

    @Override
    public StoreStats storeStats(GeneralId id) {
        return storeStatsMap.get(id);
    }

    @Override
    public BigDecimal storeScore(GeneralId id) {
        return metaStore.executorView(id).score().score();
    }

    private Optional<GeneralId> storeIdForEndpoint(Endpoint endpoint) {
        return Optional.ofNullable(endpointStoreId.get(endpoint))
            .ifAbsentSet(() -> metaStore.namespaceView().resourceViews()
                .keySet()
                .stream()
                .filter(id -> GeneralIdHelper.storeName(endpoint).equals(id.name()))
                .findAny()
                .orElse(null));
    }

    @Override
    public void saveStore(Store store) {
        GeneralId id = GeneralId.fromStr(store.getId());
        ExecutorView view = mapping(store);
        metaStore.updateExecutorView(view);
        storeMap.put(id, store);
        endpointStoreId.put(store.getEndpoint(), id);
        storeRegion.put(
            id,
            store.getRegions().stream()
                .map(Region::getId)
                .map(GeneralIdHelper::region)
                .collect(Collectors.toCollection(ConcurrentHashSet::new))
        );
    }

    @Override
    public void saveRegionHeartbeat(Region region, RegionStats regionStats) {
        final GeneralId regionId = GeneralIdHelper.region(region.getId());
        RegionApp regionApp = mapping(region);
        RegionView regionView = mapping(regionApp, regionStats);
        GeneralId storeId = GeneralId.fromStr(regionStats.getLeader().getStoreId());
        Set<GeneralId> nodes = region.getPeers()
            .stream()
            .map(Peer::getEndpoint)
            .map(metaStore::storeId)
            .collect(Collectors.toSet());
        regionView.nodes(nodes);
        regionView.leader(storeId);
        regionView.followers(nodes);
        metaStore.updateRegionView(regionApp, regionView);
        updateLeader(regionId, storeId);
        regionMap.put(regionId, region);
        regionStatsMap.put(GeneralId.appViewOf(regionId.seqNo(), regionId.name()), regionStats);
    }

    @Override
    public String newRegionId() {
        return GeneralIdHelper.region(metaStore.newAppSeq().join()).toString();
    }

    private void updateLeader(GeneralId regionId, GeneralId storeId) {
        Optional.ofNullable(regionLeaderMap.get(regionId))
            .filter(storeId::equals)
            .ifPresent(oid -> regionLeaderMap.put(regionId, storeId))
            .map(storeLeaderRegion::get)
            .ifPresent(s -> s.remove(regionId));

        Optional.ofNullable(storeLeaderRegion.get(storeId))
            .ifAbsentSet(() -> storeLeaderRegion.computeIfAbsent(storeId, k -> new ConcurrentHashSet<>()))
            .ifPresent(s -> s.add(regionId));
    }

    @Override
    public void saveStoreStats(StoreStats storeStats) {
        GeneralId id = GeneralId.fromStr(storeStats.getStoreId());
        ExecutorView view = metaStore.executorView(id);
        view.stats(storeStats);
        metaStore.updateExecutorView(view);
        storeStatsMap.put(id, storeStats);
    }

    public Store mapping(ExecutorView executorView) {
        if (executorView == null) {
            return null;
        }
        Store store = new Store();
        store.setEndpoint(new Endpoint(executorView.location().getHost(), executorView.location().getPort()));
        store.setId(executorView.resourceId().toString());
        store.setLabels(executorView
            .labels().entrySet().stream()
            .map(e -> new StoreLabel(e.getKey(), e.getValue()))
            .collect(Collectors.toList())
        );
        store.setRegions(
            executorView.apps().stream()
                .map(id -> metaStore.namespace().<RegionApp>getApp(id))
                .map(this::mapping)
                .collect(Collectors.toList())
        );
        return store;
    }

    public ExecutorView mapping(Store store) {
        if (store == null) {
            return null;
        }
        GeneralId generalId = GeneralId.fromStr(store.getId());
        ExecutorView view = new ExecutorView(generalId, store.getEndpoint());
        store.getRegions().stream().map(Region::getId).map(GeneralIdHelper::region).forEach(view::addApp);
        return view;
    }

    public Region mapping(RegionApp regionApp) {
        if (regionApp == null) {
            return null;
        }
        String regionId = regionApp.regionId();

        RegionView regionView = metaStore.regionView(regionApp.view());
        List<Peer> peerIds = regionView.nodeResources().stream()
            .map(id -> metaStore.namespaceView().<ExecutorView>getResourceView(id))
            .map(v -> new Peer(regionId, v.resourceId().toString(), new Endpoint(v.getHost(), v.getPort())))
            .collect(Collectors.toList());

        return new Region(
            regionId,
            regionApp.startKey(),
            regionApp.endKey(),
            new RegionEpoch(regionApp.version(), regionView.confVer()),
            peerIds
        );
    }

    public RegionApp mapping(Region region) {
        if (region == null) {
            return null;
        }
        return createRegionApp(GeneralIdHelper.region(region.getId()), region);
    }

    public RegionView mapping(RegionApp app, RegionStats stats) {
        if (app == null || stats == null) {
            return null;
        }
        RegionView view = new RegionView(GeneralIdHelper.regionView(app.appId().seqNo()), app.appId(), stats);
        app.view(view.viewId());
        return view;
    }

    private RegionApp createRegionApp(
        GeneralId generalId, Region region
    ) {
        return new RegionApp(
            region.getId(),
            generalId,
            region.getStartKey(),
            region.getEndKey(),
            region.getRegionEpoch().getVersion()
        );
    }

}
