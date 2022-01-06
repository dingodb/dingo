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

package io.dingodb.coordinator.namespace.impl;

import io.dingodb.coordinator.GeneralId;
import io.dingodb.coordinator.app.App;
import io.dingodb.coordinator.app.AppView;
import io.dingodb.coordinator.namespace.NamespaceView;
import io.dingodb.coordinator.resource.ResourceView;
import io.dingodb.coordinator.resource.impl.ExecutorView;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class NamespaceViewImpl implements NamespaceView {

    private final String namespace;
    private Map<GeneralId, ExecutorView> resourceViews = new HashMap<>();
    private Map<GeneralId, AppView<?, ?>> appViews = new HashMap<>();

    public synchronized void updateAppView(AppView<?, ?> appView) {
        appViews.put(appView.viewId(), appView);
    }

    public synchronized void updateResourceView(ExecutorView resourceView) {
        resourceViews.put(resourceView.resourceId(), resourceView);
    }

    @Override
    public <V extends AppView<V, A>, A extends App<A, V>> V getAppView(GeneralId id) {
        return (V) appViews.get(id);
    }

    @Override
    public <R extends ResourceView> R getResourceView(GeneralId id) {
        return (R) resourceViews.get(id);
    }

    @Override
    public Map<GeneralId, ExecutorView> resourceViews() {
        return resourceViews;
    }

    @Override
    public Map<GeneralId, AppView<?, ?>> appViews() {
        return appViews;
    }
}
