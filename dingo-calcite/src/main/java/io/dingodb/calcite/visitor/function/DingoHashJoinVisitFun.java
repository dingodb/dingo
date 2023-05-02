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

import io.dingodb.calcite.rel.DingoHashJoin;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.HashJoinOperator;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.dingodb.calcite.rel.DingoRel.dingo;

public class DingoHashJoinVisitFun {
    @NonNull
    public static List<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, @NonNull DingoHashJoin rel
    ) {
        Collection<Output> leftInputs = dingo(rel.getLeft()).accept(visitor);
        Collection<Output> rightInputs = dingo(rel.getRight()).accept(visitor);
        Map<Id, Output> leftInputsMap = new HashMap<>(leftInputs.size());
        Map<Id, Output> rightInputsMap = new HashMap<>(rightInputs.size());
        // Only one left input in each task, because of coalescing.
        leftInputs.forEach(i -> leftInputsMap.put(i.getTaskId(), i));
        rightInputs.forEach(i -> rightInputsMap.put(i.getTaskId(), i));
        List<Output> outputs = new LinkedList<>();
        for (Map.Entry<Id, Output> entry : leftInputsMap.entrySet()) {
            Id taskId = entry.getKey();
            Output left = entry.getValue();
            Output right = rightInputsMap.get(taskId);
            JoinInfo joinInfo = rel.analyzeCondition();
            Operator operator = new HashJoinOperator(TupleMapping.of(joinInfo.leftKeys),
                TupleMapping.of(joinInfo.rightKeys), rel.getLeft().getRowType().getFieldCount(),
                rel.getRight().getRowType().getFieldCount(),
                rel.getJoinType() == JoinRelType.LEFT || rel.getJoinType() == JoinRelType.FULL,
                rel.getJoinType() == JoinRelType.RIGHT || rel.getJoinType() == JoinRelType.FULL
            );
            operator.setId(idGenerator.get());
            left.setLink(operator.getInput(0));
            right.setLink(operator.getInput(1));
            Task task = job.getTask(taskId);
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }
}
