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

import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.util.NoBreakFunctionWrapper;
import io.dingodb.common.util.Optional;
import io.dingodb.coordinator.GeneralId;
import io.dingodb.coordinator.app.impl.RegionApp;
import io.dingodb.coordinator.app.impl.RegionView;
import io.dingodb.coordinator.meta.GeneralIdHelper;
import io.dingodb.coordinator.meta.Mappings;
import io.dingodb.coordinator.meta.MetaStore;
import io.dingodb.coordinator.namespace.Namespace;
import io.dingodb.coordinator.namespace.impl.NamespaceImpl;
import io.dingodb.coordinator.namespace.impl.NamespaceViewImpl;
import io.dingodb.coordinator.resource.impl.ExecutorView;
import io.dingodb.coordinator.store.AsyncKeyValueStore;
import io.dingodb.dingokv.metadata.Cluster;
import io.dingodb.dingokv.metadata.Store;
import io.dingodb.dingokv.serialization.Serializer;
import io.dingodb.dingokv.serialization.Serializers;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.dingodb.expr.json.runtime.Parser.JSON;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class MetaStoreImpl implements MetaStore {

    public static final String META = "meta";

    public static final String SEQ = "seq";
    public static final String ANY = "*";

    public static final byte[] APPS_KEY = GeneralId.appOf(0L, ANY).toString().getBytes(UTF_8);
    public static final byte[] APPS_VIEW_KEY = GeneralId.appViewOf(0L, ANY).toString().getBytes(UTF_8);
    public static final byte[] RESOURCES_KEY = GeneralId.resourceViewOf(0L, ANY).toString().getBytes(UTF_8);

    public static final byte[] APP_SEQ_KEY = GeneralId.appOf(0L, SEQ).toString().getBytes(UTF_8);
    public static final byte[] RESOURCE_SEQ_KEY = GeneralId.resourceViewOf(0L, SEQ).toString().getBytes(UTF_8);

    public static final String APP_VIEW = "app.view";
    public static final String RESOURCE_VIEW = "resource.view";

    private final AsyncKeyValueStore store;
    private Serializer serializer = Serializers.getDefault();

    private NamespaceImpl namespace;
    private NamespaceViewImpl namespaceView;

    private Mappings mappings = new Mappings(this);

    public MetaStoreImpl(AsyncKeyValueStore store) {
        this.store = store;
    }

    @Override
    public void init() throws Exception {
        List<GeneralId> apps = apps();
        namespace = new NamespaceImpl(META);
        namespaceView = new NamespaceViewImpl(META);
        apps.stream()
            .map(this::regionApp)
            .filter(Objects::nonNull)
            .forEach(namespace::updateApp);
        apps.stream()
            .map(GeneralId::seqNo)
            .map(GeneralIdHelper::regionView)
            .map(this::regionView)
            .filter(Objects::nonNull)
            .forEach(namespaceView::updateAppView);
        namespaceView.appViews().values().forEach(v -> namespace.getApp(v.viewId()));
        resourceViews().stream()
            .map(this::executorView)
            .filter(Objects::nonNull)
            .peek(executorView -> executorView.addAllApp(apps))
            .forEach(namespaceView::updateResourceView);
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
    public Mappings mappings() {
        return mappings;
    }

    @Override
    @Deprecated
    public Cluster cluster() {
        List<Store> stores = namespaceView.resourceViews().values().stream()
            .filter(ExecutorView.class::isInstance)
            .map(ExecutorView.class::cast)
            .map(mappings::mapping)
            .collect(Collectors.toList());
        Cluster cluster = new Cluster(0, stores);
        log.info("Get cluster, cluster: {}", cluster);
        return cluster;
    }

    @Override
    public GeneralId storeId(Endpoint endpoint) {
        GeneralId generalId = namespaceView.resourceViews()
            .keySet()
            .stream()
            .filter(id -> GeneralIdHelper.storeName(endpoint).equals(id.name()))
            .findAny()
            .orElseGet(() -> GeneralIdHelper.store(newResourceSeq().join(), endpoint));
        log.info("Get store id by endpoint, endpoint: [{}], store id: [{}]", endpoint, generalId);
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
            log.info("Get executor view by id, id: [{}], view: {}", id, view);
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
            log.info("Get region app by id, id: [{}], region app: {}", id, regionApp);
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
            log.info("Get region app by id, id: [{}], region view: {}", id, regionView);
            return regionView;
        } catch (Exception e) {
            log.error("Get region view by id, id: [{}]", id, e);
        }
        return null;
    }

    @Override
    public void updateRegionView(RegionApp regionApp, RegionView regionView) {
        log.info("Update region view for region app and region view, app: {}, view: {}", regionApp, regionView);
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
        log.info("Update executor view, view: {}", executorView);
        Optional.ofNullable(namespaceView.<ExecutorView>getResourceView(executorView.resourceId()))
            .ifAbsent(() -> saveExecutorView(executorView))
            .ifPresent(v -> v.stats(executorView.stats()))
            .ifPresent(v -> v.addAllApp(executorView.apps()))
            .ifPresent(this::saveExecutorView);
    }

    private ExecutorView newExecutorView(GeneralId id, Endpoint endpoint) {
        ExecutorView view = new ExecutorView(id, endpoint);
        updateExecutorView(view);
        log.info("Create executor view for id and endpoint, id: [{}], endpoint: [{}]", id, endpoint);
        return view;
    }

    @Override
    public CompletableFuture<Long> newResourceSeq() {
        CompletableFuture<Long> increment = store.increment(RESOURCE_SEQ_KEY);
        increment.thenAccept(x -> System.out.println("resource" + x));
        return increment;
    }

    @Override
    public CompletableFuture<Long> newAppSeq() {
        CompletableFuture<Long> increment = store.increment(APP_SEQ_KEY);
        increment.thenAccept(x -> System.out.println("app" + x));
        return increment;
    }

    private AtomicLong getSeq(byte[] key) throws Exception {
        byte[] bytes = store.get(key).get();
        if (bytes == null) {
            return new AtomicLong(0);
        }
        return new AtomicLong(PrimitiveCodec.readVarLong(bytes));
    }

    private CompletableFuture<RegionApp> saveRegion(RegionApp app) {
        log.info("Save region app, app: {}", app);
        CompletableFuture<RegionApp> future = new CompletableFuture<>();
        byte[] idKey = app.appId().toString().getBytes(UTF_8);
        store.getAndPut(idKey, serializer.writeObject(app))
            .whenComplete((r, e) -> {
                if (e == null) {
                    if (r == null) {
                        store.merge(APPS_KEY, idKey).whenComplete((r1, e1) -> {
                            if (e1 == null) {
                                future.complete(null);
                            } else {
                                future.completeExceptionally(e1);
                            }
                        });
                    } else {
                        future.complete(serializer.readObject(r, RegionApp.class));
                    }
                    namespace.updateApp(app);
                } else {
                    future.completeExceptionally(e);
                }
            });
        return future;
    }

    private byte[] saveRegionView(RegionView appView) {
        log.info("Save region view, view: {}", appView);
        byte[] idKey = appView.viewId().toString().getBytes(UTF_8);
        CompletableFuture<byte[]> future = store.getAndPut(idKey, serializer.writeObject(appView));
        future.thenAccept(r -> namespaceView.updateAppView(appView));
        return future.join();
    }

    private CompletableFuture<ExecutorView> saveExecutorView(ExecutorView executorView) {
        log.info("Save region executor view, view: {}", executorView);
        CompletableFuture<ExecutorView> future = new CompletableFuture<>();
        byte[] idKey = executorView.resourceId().toString().getBytes(UTF_8);
        store.getAndPut(idKey, serializer.writeObject(executorView))
            .whenComplete((r, e) -> {
                if (e == null) {
                    if (r == null) {
                        store.merge(RESOURCES_KEY, idKey).whenComplete((r1, e1) -> {
                            if (e1 == null) {
                                future.complete(null);
                            } else {
                                future.completeExceptionally(e1);
                            }
                        });
                    } else {
                        future.complete(serializer.readObject(r, ExecutorView.class));
                    }
                    namespaceView.updateResourceView(executorView);
                } else {
                    future.completeExceptionally(e);
                }
            });
        return future;
    }

    private List<GeneralId> apps() {
        try {
            byte[] bytes = store.get(APPS_KEY).get();
            if (bytes == null) {
                return Collections.emptyList();
            }
            return Arrays.stream(BytesUtil.readUtf8(bytes).split(","))
                .map(GeneralId::fromStr)
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<GeneralId> appViews() {
        try {
            byte[] bytes = store.get(APPS_VIEW_KEY).get();
            if (bytes == null) {
                return Collections.emptyList();
            }
            return Arrays.stream(BytesUtil.readUtf8(bytes).split(","))
                .map(GeneralId::fromStr)
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<GeneralId> resourceViews() {
        try {
            byte[] bytes = store.get(RESOURCES_KEY).get();
            if (bytes == null) {
                return Collections.emptyList();
            }
            return Arrays.stream(BytesUtil.readUtf8(bytes).split(","))
                .map(GeneralId::fromStr)
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T get(GeneralId id, Class<T> cls) {
        try {
            return JSON.parse(new ByteArrayInputStream(store.get(id.toString().getBytes(UTF_8)).get()), cls);
        } catch (Exception e) {
            return null;
        }
    }
}
