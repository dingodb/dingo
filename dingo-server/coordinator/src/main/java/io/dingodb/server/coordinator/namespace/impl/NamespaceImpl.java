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

package io.dingodb.server.coordinator.namespace.impl;

import io.dingodb.server.coordinator.GeneralId;
import io.dingodb.server.coordinator.app.App;
import io.dingodb.server.coordinator.namespace.Namespace;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class NamespaceImpl implements Namespace {

    private final String namespace;
    private Map<GeneralId, App<?, ?>> apps = new HashMap<>();

    public synchronized void updateApp(App<?, ?> app) {
        apps.put(app.appId(), app);
    }

    @Override
    public <A extends App<A, ?>> A getApp(GeneralId id) {
        return (A) apps.get(id);
    }

}
