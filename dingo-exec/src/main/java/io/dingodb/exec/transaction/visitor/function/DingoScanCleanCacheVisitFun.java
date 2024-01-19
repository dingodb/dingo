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

package io.dingodb.exec.transaction.visitor.function;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.params.ScanCacheParam;
import io.dingodb.exec.transaction.params.ScanCleanCacheParam;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.exec.transaction.visitor.data.ScanCacheLeaf;
import io.dingodb.exec.transaction.visitor.data.ScanCleanCacheLeaf;
import io.dingodb.net.Channel;
import io.dingodb.store.api.transaction.data.IsolationLevel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.dingodb.exec.utils.OperatorCodeUtils.SCAN_CACHE;

public class DingoScanCleanCacheVisitFun {

    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, ITransaction transaction,
        DingoTransactionRenderJob visitor, ScanCleanCacheLeaf scanCleanCacheLeaf) {
        List<Vertex> outputs = new ArrayList<>();
        DingoType dingoType = DingoTypeFactory.tuple(new DingoType[]{new BooleanType(true)});
        Map<CommonId, Channel> channelMap = transaction.getChannelMap();
        for (Map.Entry<CommonId, Channel> channelEntry : channelMap.entrySet()) {
            Location remoteLocation = channelEntry.getValue().remoteLocation();
            ScanCleanCacheParam param = new ScanCleanCacheParam(dingoType, transaction.getType());
            Vertex vertex = new Vertex(SCAN_CACHE, param);
            Task task = job.getOrCreate(
                remoteLocation,
                idGenerator,
                transaction.getType(),
                IsolationLevel.of(transaction.getIsolationLevel())
            );
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            outputs.add(vertex);
        }
        ScanCleanCacheParam param = new ScanCleanCacheParam(dingoType, transaction.getType());
        Vertex vertex = new Vertex(SCAN_CACHE, param);
        Task task = job.getOrCreate(
            currentLocation,
            idGenerator,
            transaction.getType(),
            IsolationLevel.of(transaction.getIsolationLevel())
        );
        vertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(vertex);
        outputs.add(vertex);
        return outputs;
    }
}
