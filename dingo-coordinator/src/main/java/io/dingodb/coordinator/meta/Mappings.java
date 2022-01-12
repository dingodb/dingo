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
import io.dingodb.coordinator.app.impl.RegionApp;
import io.dingodb.coordinator.app.impl.RegionView;
import io.dingodb.coordinator.resource.impl.ExecutorView;
import io.dingodb.store.row.metadata.Peer;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.store.row.metadata.RegionEpoch;
import io.dingodb.store.row.metadata.RegionStats;
import io.dingodb.store.row.metadata.Store;
import io.dingodb.store.row.metadata.StoreLabel;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class Mappings {

    private final MetaStore metaStore;

    public Mappings(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    public Store mapping(ExecutorView executorView) {
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
        GeneralId generalId = GeneralId.fromStr(store.getId());
        ExecutorView view = new ExecutorView(generalId, store.getEndpoint());
        store.getRegions().stream().map(Region::getId).map(GeneralIdHelper::region).forEach(view::addApp);
        return view;
    }

    public Region mapping(RegionApp regionApp) {
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
        return createRegionApp(GeneralIdHelper.region(region.getId()), region);
    }

    public RegionView mapping(RegionApp app, RegionStats stats) {
        RegionView view = new RegionView(GeneralIdHelper.regionView(app.appId().seqNo()), app.appId(), stats);
        app.view(view.viewId());
        return view;
    }

    @Nonnull
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
