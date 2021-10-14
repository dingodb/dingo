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

package io.dingodb.helix.part.impl;

import io.dingodb.helix.part.HelixSpectatorPart;
import io.dingodb.helix.part.ZkHelixPart;
import io.dingodb.server.ServerConfiguration;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.model.ResourceConfig;

import java.util.List;

public class ZkHelixSpectatorPart extends ZkHelixPart implements HelixSpectatorPart, ResourceConfigChangeListener {

    public ZkHelixSpectatorPart(
        ServerConfiguration configuration
    ) {
        super(InstanceType.SPECTATOR, configuration);
    }

    @Override
    public void init() throws Exception {
        super.init();
    }

    @Override
    public void onResourceConfigChange(List<ResourceConfig> resourceConfigs, NotificationContext context) {
    }
}
