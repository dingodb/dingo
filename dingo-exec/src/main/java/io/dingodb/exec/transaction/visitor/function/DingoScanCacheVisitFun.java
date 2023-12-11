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
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.exec.transaction.visitor.data.ScanCacheLeaf;
import io.dingodb.net.Channel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.dingodb.exec.utils.OperatorCodeUtils.SCAN_CACHE;

public class DingoScanCacheVisitFun {

    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, ITransaction transaction, DingoTransactionRenderJob visitor, ScanCacheLeaf scanCacheLeaf) {
        List<Vertex> outputs = new ArrayList<>();
//        if(scanCacheLeaf.getData() instanceof Element) {
//            ScanCacheOperator operator = new ScanCacheOperator(transaction.getCache());
//            Collection<Output> inputs = scanCacheLeaf.getData().accept(visitor);
//            Output input = sole(inputs);
//            Task task = input.getTask();
//            operator.setId(idGenerator.getOperatorId(task.getId()));
//            task.putOperator(operator);
//            input.setLink(operator.getInput(0));
//        } else {
        DingoType dingoType = DingoTypeFactory.tuple(new DingoType[]{new BooleanType(true)});
        Map<CommonId, Channel> channelMap = transaction.getChannelMap();
//            if(channelMap.size() == 0){
//                Location remoteLocation = new Location("172.20.3.30", 10000);
//                ScanCacheOperator operator = new ScanCacheOperator(dingoType);
//                Task task = job.getOrCreate(remoteLocation, idGenerator);
//                operator.setId(idGenerator.getOperatorId(task.getId()));
//                task.putOperator(operator);
//                outputs.addAll(operator.getOutputs());
//            }
        for (Map.Entry<CommonId, Channel> channelEntry : channelMap.entrySet()) {
            Location remoteLocation = channelEntry.getValue().remoteLocation();
            ScanCacheParam param = new ScanCacheParam(dingoType);
            Vertex vertex = new Vertex(SCAN_CACHE, param);
            Task task = job.getOrCreate(remoteLocation, idGenerator);
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            outputs.add(vertex);
        }
        ScanCacheParam param = new ScanCacheParam(dingoType, transaction.getCache());
        Vertex vertex = new Vertex(SCAN_CACHE, param);
        Task task = job.getOrCreate(currentLocation, idGenerator);
        vertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(vertex);
        outputs.add(vertex);
//        }
        return outputs;
    }
}
