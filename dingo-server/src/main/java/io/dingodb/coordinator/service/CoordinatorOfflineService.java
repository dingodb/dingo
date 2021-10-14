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

package io.dingodb.coordinator.service;

import io.dingodb.helix.service.AbstractOfflineService;
import io.dingodb.helix.service.StateServiceContext;
import io.dingodb.net.NetService;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;

public class CoordinatorOfflineService extends AbstractOfflineService {

    private final NetService netService;

    public CoordinatorOfflineService(StateServiceContext context, NetService netService) {
        super(context);
        this.netService = netService;
    }

    @Override
    public void start(Message stateMessage, NotificationContext context) throws Exception {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
