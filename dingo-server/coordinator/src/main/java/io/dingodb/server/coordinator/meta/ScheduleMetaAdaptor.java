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


import com.alipay.sofa.jraft.util.Endpoint;
import io.dingodb.server.coordinator.GeneralId;
import io.dingodb.server.coordinator.app.impl.RegionApp;
import io.dingodb.server.coordinator.app.impl.RegionView;
import io.dingodb.server.coordinator.namespace.Namespace;
import io.dingodb.server.coordinator.namespace.NamespaceView;
import io.dingodb.server.coordinator.resource.impl.ExecutorView;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface ScheduleMetaAdaptor extends MetaAdaptor {

    NamespaceView namespaceView();

    Namespace namespace();

    RegionApp regionApp(GeneralId id);

    RegionView regionView(GeneralId id);

    ExecutorView executorView(GeneralId id);

    ExecutorView executorView(Endpoint endpoint);

    GeneralId storeId(Endpoint endpoint);

    void updateRegionView(RegionApp regionApp, RegionView regionView);

    void updateRegionApp(RegionApp regionApp);

    void updateExecutorView(ExecutorView executorView);

    CompletableFuture<Long> newResourceSeq();

    CompletableFuture<Long> newAppSeq();

}
