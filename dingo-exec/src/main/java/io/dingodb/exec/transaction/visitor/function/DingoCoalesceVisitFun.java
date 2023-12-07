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
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.CoalesceOperator;
import io.dingodb.exec.operator.SumUpOperator;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.exec.transaction.visitor.data.StreamConverterLeaf;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DingoCoalesceVisitFun {
    public static Collection<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, ITransaction transaction,
        DingoTransactionRenderJob visitor, @NonNull Collection<Output> inputs, StreamConverterLeaf streamConverterLeaf) {
        // Coalesce inputs from the same task. taskId --> list of inputs
        Map<CommonId, List<Output>> inputsMap = new HashMap<>();
        for (Output input : inputs) {
            CommonId taskId = input.getTaskId();
            List<Output> list = inputsMap.computeIfAbsent(taskId, k -> new LinkedList<>());
            list.add(input);
        }
        List<Output> outputs = new LinkedList<>();
        for (Map.Entry<CommonId, List<Output>> entry : inputsMap.entrySet()) {
            List<Output> list = entry.getValue();
            int size = list.size();
            if (size <= 1) {
                // Need no coalescing.
                outputs.addAll(list);
            } else {
                Output one = list.get(0);
                Task task = one.getTask();
                Operator operator = new CoalesceOperator(size);
                operator.setId(idGenerator.getOperatorId(task.getId()));
                task.putOperator(operator);
                int i = 0;
                for (Output input : list) {
                    input.setLink(operator.getInput(i));
                    ++i;
                }
                Output newOutput = operator.getSoleOutput();
                newOutput.copyHint(one);
                if (one.isToSumUp()) {
                    Operator sumUpOperator = new SumUpOperator();
                    sumUpOperator.setId(idGenerator.getOperatorId(task.getId()));
                    task.putOperator(sumUpOperator);
                    operator.getSoleOutput().setLink(sumUpOperator.getInput(0));
                    sumUpOperator.getSoleOutput().copyHint(newOutput);
                    outputs.add(sumUpOperator.getSoleOutput());
                } else {
                    outputs.add(newOutput);
                }
            }
        }
        return outputs;
    }
}
