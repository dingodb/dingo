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

package io.dingodb.server.coordinator.meta.impl;

import io.dingodb.common.util.NoBreakFunctionWrapper;
import io.dingodb.common.util.Optional;
import io.dingodb.raft.util.BytesUtil;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.server.coordinator.GeneralId;
import io.dingodb.server.coordinator.app.impl.RegionApp;
import io.dingodb.server.coordinator.app.impl.RegionView;
import io.dingodb.server.coordinator.meta.GeneralIdHelper;
import io.dingodb.server.coordinator.meta.ScheduleMetaAdaptor;
import io.dingodb.server.coordinator.namespace.Namespace;
import io.dingodb.server.coordinator.namespace.impl.NamespaceImpl;
import io.dingodb.server.coordinator.namespace.impl.NamespaceViewImpl;
import io.dingodb.server.coordinator.resource.impl.ExecutorView;
import io.dingodb.server.coordinator.store.AsyncKeyValueStore;
import io.dingodb.store.row.serialization.Serializer;
import io.dingodb.store.row.serialization.Serializers;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.expr.json.runtime.Parser.JSON;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class ScheduleMetaAdaptorImpl extends AbstractMetaAdaptor implements ScheduleMetaAdaptor {

    public static final String META = "meta";

    public static final String SEQ = "seq";
    public static final String ANY = "*";

    public static final byte[] APP_SEQ_KEY = GeneralId.appOf(0L, SEQ).toString().getBytes(UTF_8);
    public static final byte[] RESOURCE_SEQ_KEY = GeneralId.resourceViewOf(0L, SEQ).toString().getBytes(UTF_8);

    private final AsyncKeyValueStore store;
    private Serializer serializer = Serializers.getDefault();

    private NamespaceImpl namespace;
    private NamespaceViewImpl namespaceView;

    public ScheduleMetaAdaptorImpl(AsyncKeyValueStore store) {
        this.store = store;
        this.namespace = new NamespaceImpl(META);
        this.namespaceView = new NamespaceViewImpl(META);
    }

    @Override
    public void init() {
        scanAllRegionApp()
            .whenCompleteAsync((apps, e) -> apps.forEach(namespace::updateApp))
            .whenCompleteAsync((apps, e) -> apps.stream()
                .map(RegionApp::view)
                .map(this::regionView)
                .filter(Objects::nonNull)
                .forEach(namespaceView::updateAppView));
        scanAllExecutorView()
            .whenCompleteAsync((executorViews, e) -> executorViews.forEach(namespaceView::updateResourceView));
    }

    @Override
    public Namespace namespace() {
        return namespace;
    }

    @Override
    public NamespaceViewImpl namespaceView() {
        return namespaceView;
    }

    @Override
    public GeneralId storeId(Endpoint endpoint) {
        GeneralId generalId = namespaceView.resourceViews()
            .keySet()
            .stream()
            .filter(id -> GeneralIdHelper.storeName(endpoint).equals(id.name()))
            .findAny()
            .orElseGet(() -> GeneralIdHelper.store(newResourceSeq().join(), endpoint));
        if (log.isDebugEnabled()) {
            log.debug("Get store id by endpoint, endpoint: [{}], store id: [{}]", endpoint, generalId);
        }
        return generalId;
    }

    @Override
    public ExecutorView executorView(GeneralId id) {
        try {
            ExecutorView view = Optional.ofNullable(namespaceView.<ExecutorView>getResourceView(id))
                .ifAbsentSet(() -> Optional.ofNullable(store.get(id.toString().getBytes(UTF_8)).join())
                    .map(bytes -> serializer.readObject(bytes, ExecutorView.class))
                    .orNull())
                .orNull();
            if (log.isDebugEnabled()) {
                log.debug("Get executor view by id, id: [{}], view: {}", id, view);
            }
            return view;
        } catch (Exception e) {
            log.error("Get executor view error, id: [{}]", id, e);
        }
        return null;
    }

    @Override
    public ExecutorView executorView(Endpoint endpoint) {
        GeneralId generalId = storeId(endpoint);
        return Optional.ofNullable(executorView(generalId)).orElseGet(() -> newExecutorView(generalId, endpoint));
    }

    @Override
    public RegionApp regionApp(GeneralId id) {
        try {
            RegionApp regionApp = Optional.<RegionApp>ofNullable(namespace.getApp(id))
                .ifAbsentSet(() -> Optional.ofNullable(store.get(id.toString().getBytes(UTF_8)).join())
                    .map(bytes -> serializer.readObject(bytes, RegionApp.class))
                    .orNull())
                .orNull();
            if (log.isDebugEnabled()) {
                log.debug("Get region app by id, id: [{}], region app: {}", id, regionApp);
            }
            return regionApp;
        } catch (Exception e) {
            log.error("Get region app by id, id: [{}]", id, e);
        }
        return null;
    }

    @Override
    public RegionView regionView(GeneralId id) {
        try {
            RegionView regionView = Optional.<RegionView>ofNullable(namespaceView.getAppView(id))
                .ifAbsentSet(() -> Optional.ofNullable(store.get(id.toString().getBytes(UTF_8)).join())
                    .map(bytes -> serializer.readObject(bytes, RegionView.class))
                    .ifPresent(view -> view.app(GeneralIdHelper.region(id.seqNo())))
                    .orNull())
                .orNull();
            if (log.isDebugEnabled()) {
                log.debug("Get region app by id, id: [{}], region view: {}", id, regionView);
            }
            return regionView;
        } catch (Exception e) {
            log.error("Get region view by id, id: [{}]", id, e);
        }
        return null;
    }

    @Override
    public void updateRegionView(RegionApp regionApp, RegionView regionView) {
        if (log.isDebugEnabled()) {
            log.debug("Update region view for region app and region view, app: {}, view: {}", regionApp, regionView);
        }
        updateRegionApp(regionApp);

        Optional.<RegionView>ofNullable(namespaceView.getAppView(regionApp.appId()))
            .ifAbsent(() -> saveRegionView(regionView))
            .ifPresent(view -> view.leader(regionView.leader()))
            .ifPresent(view -> view.setRegionStats(regionView.getRegionStats()))
            .ifPresent(this::saveRegionView);
        addAppToExecutorViews(regionApp.appId(), regionView.nodeResources());
    }

    private void addAppToExecutorViews(GeneralId appId, Set<GeneralId> nodes) {
        namespaceView.resourceViews().values().forEach(NoBreakFunctionWrapper.wrap(executorView -> {
            boolean containsApp = executorView.apps().contains(appId);
            boolean containsNode = nodes.contains(executorView.resourceId());
            if (containsApp && containsNode) {
                return;
            }
            if (containsApp && !containsNode) {
                executorView.apps().remove(appId);
                updateExecutorView(executorView);
            }
            if (!containsApp && containsNode) {
                executorView.addApp(appId);
                updateExecutorView(executorView);
            }
        }));
    }

    @Override
    public void updateRegionApp(RegionApp regionApp) {
        Optional.<RegionApp>ofNullable(namespace.getApp(regionApp.appId()))
            .ifAbsent(() -> saveRegion(regionApp))
            .filter(app -> regionApp.version() > app.version())
            .ifPresent(app -> app.version(regionApp.version()))
            .ifPresent(app -> app.startKey(regionApp.startKey()))
            .ifPresent(app -> app.endKey(regionApp.endKey()))
            .ifPresent(this::saveRegion);
    }

    @Override
    public void updateExecutorView(ExecutorView executorView) {
        if (log.isDebugEnabled()) {
            log.debug("Update executor view, view: {}", executorView);
        }
        Optional.ofNullable(namespaceView.<ExecutorView>getResourceView(executorView.resourceId()))
            .ifAbsent(() -> saveExecutorView(executorView))
            .ifPresent(v -> v.stats(executorView.stats()))
            .ifPresent(v -> v.addAllApp(executorView.apps()))
            .ifPresent(this::saveExecutorView);
    }

    private ExecutorView newExecutorView(GeneralId id, Endpoint endpoint) {
        ExecutorView view = new ExecutorView(id, endpoint);
        updateExecutorView(view);
        if (log.isDebugEnabled()) {
            log.debug("Create executor view for id and endpoint, id: [{}], endpoint: [{}]", id, endpoint);
        }
        return view;
    }

    @Override
    public CompletableFuture<Long> newResourceSeq() {
        return store.increment(RESOURCE_SEQ_KEY);
    }

    @Override
    public CompletableFuture<Long> newAppSeq() {
        return store.increment(APP_SEQ_KEY);
    }

    private RegionApp saveRegion(RegionApp app) {
        if (log.isDebugEnabled()) {
            log.debug("Save region app, app: {}", app);
        }
        save(app, RegionApp::appId);
        return namespace.updateApp(app);
    }

    private RegionView saveRegionView(RegionView appView) {
        if (log.isDebugEnabled()) {
            log.debug("Save region view, view: {}", appView);
        }
        save(appView, RegionView::viewId);
        return namespaceView.updateAppView(appView);
    }

    private ExecutorView saveExecutorView(ExecutorView executorView) {
        if (log.isDebugEnabled()) {
            log.debug("Save region executor view, view: {}", executorView);
        }
        save(executorView, ExecutorView::resourceId);
        return namespaceView.updateResourceView(executorView);
    }

    private <T> void save(T target, Function<T, GeneralId> getId) {
        byte[] idKey = getId.apply(target).encode();
        store.put(idKey, serializer.writeObject(target)).join();
    }

    private CompletableFuture<List<RegionApp>> scanAllRegionApp() {
        byte[] prefix = GeneralIdHelper.regionPrefix();
        System.out.println(BytesUtil.readUtf8(prefix));
        return store.scan(prefix)
            .thenApplyAsync(entries ->
                entries.stream()
                    .map(e -> serializer.readObject(e.getValue(), RegionApp.class))
                    .collect(Collectors.toList()));
    }

    private CompletableFuture<List<ExecutorView>> scanAllExecutorView() {
        byte[] prefix = GeneralIdHelper.executorViewPrefix();
        System.out.println(BytesUtil.readUtf8(prefix));
        return store.scan(prefix)
            .thenApplyAsync(entries ->
                entries.stream()
                    .map(e -> serializer.readObject(e.getValue(), ExecutorView.class))
                    .collect(Collectors.toList()));
    }

    public <T> T get(GeneralId id, Class<T> cls) {
        try {
            return JSON.parse(new ByteArrayInputStream(store.get(id.toString().getBytes(UTF_8)).get()), cls);
        } catch (Exception e) {
            return null;
        }
    }
}
