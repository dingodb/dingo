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

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.rel.dingo.DingoReduceAggregate;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.ReduceRelOpParam;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.common.util.Utils.sole;
import static io.dingodb.exec.utils.OperatorCodeUtils.REDUCE_REL_OP;

public final class DingoReduceAggregateVisitFun {
    private DingoReduceAggregateVisitFun() {
    }

    public static @NonNull Collection<Vertex> visit(
        Job job,
        @NonNull IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        @NonNull DingoReduceAggregate rel
    ) {
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);
        ReduceRelOpParam param = new ReduceRelOpParam(
            rel.getRelOp(),
            DefinitionMapper.mapToDingoType(rel.getOriginalInputType())
        );
        Vertex vertex = new Vertex(REDUCE_REL_OP, param);
        Vertex input = sole(inputs);
        Task task = input.getTask();
        vertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(vertex);
        input.setPin(0);
        Edge edge = new Edge(input, vertex);
        input.addEdge(edge);
        vertex.addIn(edge);
        return ImmutableList.of(vertex);
    }
}
