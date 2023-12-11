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
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.CoalesceParam;
import io.dingodb.exec.operator.params.SumUpParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.exec.transaction.visitor.data.StreamConverterLeaf;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.dingodb.exec.utils.OperatorCodeUtils.COALESCE;
import static io.dingodb.exec.utils.OperatorCodeUtils.SUM_UP;

public class DingoCoalesceVisitFun {
    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, ITransaction transaction,
        DingoTransactionRenderJob visitor, @NonNull Collection<Vertex> inputs, StreamConverterLeaf streamConverterLeaf
    ) {
        // Coalesce inputs from the same task. taskId --> list of inputs
        Map<CommonId, List<Vertex>> inputsMap = new HashMap<>();
        for (Vertex input : inputs) {
            CommonId taskId = input.getTaskId();
            List<Vertex> list = inputsMap.computeIfAbsent(taskId, k -> new LinkedList<>());
            list.add(input);
        }
        List<Vertex> outputs = new LinkedList<>();
        for (Map.Entry<CommonId, List<Vertex>> entry : inputsMap.entrySet()) {
            List<Vertex> list = entry.getValue();
            int size = list.size();
            if (size <= 1) {
                // Need no coalescing.
                outputs.addAll(list);
            } else {
                Vertex one = list.get(0);
                Task task = one.getTask();
                CoalesceParam coalesceParam = new CoalesceParam(size);
                Vertex coalesce = new Vertex(COALESCE, coalesceParam);
                coalesce.setId(idGenerator.getOperatorId(task.getId()));
                task.putVertex(coalesce);
                int i = 0;
                for (Vertex input : list) {
                    input.addEdge(new Edge(input, coalesce));
                    input.setPin(i);
                    ++i;
                }
                coalesce.copyHint(one);
                Edge edge = new Edge(one, coalesce);
                coalesce.addIn(edge);
                if (one.isToSumUp()) {
                    SumUpParam sumUpParam = new SumUpParam();
                    Vertex sumUp = new Vertex(SUM_UP, sumUpParam);
                    sumUp.setId(idGenerator.getOperatorId(task.getId()));
                    task.putVertex(sumUp);
                    sumUp.copyHint(coalesce);
                    Edge sumUpEdge = new Edge(coalesce, sumUp);
                    coalesce.addEdge(sumUpEdge);
                    sumUp.addIn(sumUpEdge);
                    outputs.add(sumUp);
                } else {
                    outputs.add(coalesce);
                }
            }
        }
        return outputs;
    }
}
