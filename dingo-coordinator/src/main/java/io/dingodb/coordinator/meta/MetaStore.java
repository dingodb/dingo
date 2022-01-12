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
import io.dingodb.coordinator.namespace.Namespace;
import io.dingodb.coordinator.namespace.NamespaceView;
import io.dingodb.coordinator.resource.impl.ExecutorView;
import io.dingodb.store.row.metadata.Cluster;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface MetaStore {

    void init() throws ExecutionException, InterruptedException, Exception;

    NamespaceView namespaceView();

    Namespace namespace();

    RegionApp regionApp(GeneralId id);

    RegionView regionView(GeneralId id);

    ExecutorView executorView(GeneralId id);

    ExecutorView executorView(Endpoint endpoint);

    GeneralId storeId(Endpoint endpoint);

    Cluster cluster();

    void updateRegionView(RegionApp regionApp, RegionView regionView);

    void updateRegionApp(RegionApp regionApp);

    void updateExecutorView(ExecutorView executorView);

    Mappings mappings();

    CompletableFuture<Long> newResourceSeq();

    CompletableFuture<Long> newAppSeq();

}
