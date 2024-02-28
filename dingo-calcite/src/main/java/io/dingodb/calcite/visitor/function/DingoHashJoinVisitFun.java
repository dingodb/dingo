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

import io.dingodb.calcite.rel.dingo.DingoHashJoin;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.HashJoinParam;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.exec.utils.OperatorCodeUtils.HASH_JOIN;

public class DingoHashJoinVisitFun {
    @NonNull
    public static List<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, @NonNull DingoHashJoin rel
    ) {
        Collection<Vertex> leftInputs = dingo(rel.getLeft()).accept(visitor);
        Collection<Vertex> rightInputs = dingo(rel.getRight()).accept(visitor);
        Map<CommonId, Vertex> leftInputsMap = new HashMap<>(leftInputs.size());
        Map<CommonId, Vertex> rightInputsMap = new HashMap<>(rightInputs.size());
        // Only one left input in each task, because of coalescing.
        leftInputs.forEach(i -> leftInputsMap.put(i.getTaskId(), i));
        rightInputs.forEach(i -> rightInputsMap.put(i.getTaskId(), i));
        List<Vertex> outputs = new LinkedList<>();
        for (Map.Entry<CommonId, Vertex> entry : leftInputsMap.entrySet()) {
            CommonId taskId = entry.getKey();
            Vertex left = entry.getValue();
            Vertex right = rightInputsMap.get(taskId);
            JoinInfo joinInfo = rel.analyzeCondition();
            HashJoinParam param = new HashJoinParam(TupleMapping.of(joinInfo.leftKeys),
                TupleMapping.of(joinInfo.rightKeys), rel.getLeft().getRowType().getFieldCount(),
                rel.getRight().getRowType().getFieldCount(),
                rel.getJoinType() == JoinRelType.LEFT || rel.getJoinType() == JoinRelType.FULL,
                rel.getJoinType() == JoinRelType.RIGHT || rel.getJoinType() == JoinRelType.FULL
            );
            Vertex vertex = new Vertex(HASH_JOIN, param);
            vertex.setId(idGenerator.getOperatorId(taskId));
            left.setPin(0);
            right.setPin(1);
            left.addEdge(new Edge(left, vertex));
            right.addEdge(new Edge(right, vertex));
            vertex.addIn(new Edge(left, vertex));
            vertex.addIn(new Edge(right, vertex));
            Task task = job.getTask(taskId);
            task.putVertex(vertex);
            outputs.add(vertex);
        }
        return outputs;
    }
}
