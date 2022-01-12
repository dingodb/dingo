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

package io.dingodb.executor.service;

import io.dingodb.common.table.TableDefinition;
import io.dingodb.exec.Services;
import io.dingodb.helix.service.AbstractFollowerService;
import io.dingodb.helix.service.StateService;
import io.dingodb.helix.service.StateServiceContext;
import io.dingodb.net.NetService;
import io.dingodb.store.api.PartitionOper;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.NotificationContext;

@Slf4j
public class ExecutorFollowerService extends AbstractFollowerService {

    private final NetService netService;
    private final StoreInstance storeInstance;

    private PartitionOper kvBlock;

    public ExecutorFollowerService(StateServiceContext context, NetService netService, StoreInstance storeInstance) {
        super(context);
        this.netService = netService;
        this.storeInstance = storeInstance;
    }

    @Override
    public void start(org.apache.helix.model.Message stateMessage, NotificationContext context) throws Exception {
        openKvBlock();
        StateService lastService = this.context.lastService();
        if (lastService != null) {
            lastService.close();
            log.info("Previous state service [{}] will close.", lastService.state());
        }
        log.info("Start {}.", getClass().getSimpleName());
    }

    private void openKvBlock() {
        TableDefinition td = Services.META.getTableDefinition(resource());
        kvBlock = storeInstance.getKvBlock(resource(), partitionId(), false);
    }

    private void closeTablePart() {
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
