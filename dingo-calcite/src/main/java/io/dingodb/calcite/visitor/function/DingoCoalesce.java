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

package io.dingodb.calcite.visitor.function;

import io.dingodb.calcite.traits.DingoRelPartition;
import io.dingodb.common.util.Optional;
import io.dingodb.common.CommonId;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.CoalesceParam;
import io.dingodb.exec.operator.params.SumUpParam;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.dingodb.exec.utils.OperatorCodeUtils.COALESCE;
import static io.dingodb.exec.utils.OperatorCodeUtils.SUM_UP;

public class DingoCoalesce {

    public static List<Vertex> coalesce(IdGenerator idGenerator, @NonNull Collection<Vertex> inputs) {
        return coalesce(idGenerator, inputs, Collections.emptySet(), Collections.emptySet());
    }
    @NonNull
    public static List<Vertex> coalesce(
        IdGenerator idGenerator,
        @NonNull Collection<Vertex> inputs,
        Set<DingoRelPartition> dstPartitions,
        Set<DingoRelPartition> srcPartitions
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
                Map<CommonId, List<Vertex>> partOutputs = list.stream()
                    .collect(Collectors.groupingBy(
                        output -> Optional.ofNullable(output.getHint())
                            .filter(!dstPartitions.isEmpty())
                            .map(OutputHint::getPartId)
                            .orElseGet(() -> CommonId.EMPTY_DISTRIBUTE)
                    ));
                for (Map.Entry<CommonId, List<Vertex>> partOutput : partOutputs.entrySet()) {
                    List<Vertex> value = partOutput.getValue();
                    int valueSize = value.size();
                    Vertex one = value.get(0);
                    Task task = one.getTask();
                    CoalesceParam coalesceParam = new CoalesceParam(valueSize);
                    Vertex coalesce = new Vertex(COALESCE, coalesceParam);
                    coalesce.setId(idGenerator.getOperatorId(task.getId()));
                    task.putVertex(coalesce);
                    int i = 0;
                    for (Vertex input : value) {
                        input.addEdge(new Edge(input, coalesce));
                        input.setPin(i);
                        i++;
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
        }
        return outputs;
    }
}
