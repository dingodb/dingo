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

package io.dingodb.server.coordinator.api;

import io.dingodb.common.CommonId;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.adaptor.impl.TableAdaptor;
import io.dingodb.server.coordinator.schedule.ClusterScheduler;
import io.dingodb.server.protocol.meta.Table;

public class ScheduleApi implements io.dingodb.server.api.ScheduleApi {

    public ScheduleApi() {
        ApiRegistry.getDefault().register(io.dingodb.server.api.ScheduleApi.class, this);
    }

    @Override
    public void autoSplit(CommonId tableId, boolean auto) {
        TableAdaptor tableAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Table.class);
        Table table = tableAdaptor.get(tableId);
        table.setAutoSplit(auto);
        tableAdaptor.pureSave(table);
    }

    @Override
    public void maxSize(CommonId tableId, long maxSize) {
        TableAdaptor tableAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Table.class);
        Table table = tableAdaptor.get(tableId);
        table.setPartMaxSize(maxSize);
        tableAdaptor.pureSave(table);
    }

    @Override
    public void maxCount(CommonId tableId, long maxCount) {
        TableAdaptor tableAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Table.class);
        Table table = tableAdaptor.get(tableId);
        table.setPartMaxCount(maxCount);
        tableAdaptor.pureSave(table);
    }

    @Override
    public void addReplica(CommonId tableId, CommonId partId, CommonId executorId) {
        ClusterScheduler.instance().getTableScheduler(tableId).addReplica(partId, executorId);
    }

    @Override
    public void removeReplica(CommonId tableId, CommonId partId, CommonId executorId) {
        ClusterScheduler.instance().getTableScheduler(tableId).removeReplica(partId, executorId);
    }

    @Override
    public void transferLeader(CommonId tableId, CommonId partId, CommonId executorId) {
        ClusterScheduler.instance().getTableScheduler(tableId).transferLeader(partId, executorId);
    }

    @Override
    public void splitPart(CommonId tableId, CommonId partId) {
        ClusterScheduler.instance().getTableScheduler(tableId).split(partId);
    }

    @Override
    public void splitPart(CommonId tableId, CommonId partId, byte[] key) {
        if (key == null) {
            splitPart(tableId, partId);
            return;
        }
        ClusterScheduler.instance().getTableScheduler(tableId).split(partId, key);
    }
}
