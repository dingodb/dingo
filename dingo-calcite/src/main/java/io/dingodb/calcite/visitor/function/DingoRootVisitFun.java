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
import io.dingodb.calcite.rel.dingo.DingoRoot;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.RootParam;
import io.dingodb.exec.transaction.base.ITransaction;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.common.util.Utils.sole;
import static io.dingodb.exec.utils.OperatorCodeUtils.ROOT;

public class DingoRootVisitFun {
    @NonNull
    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation,
        ITransaction transaction, DingoJobVisitor visitor, @NonNull DingoRoot rel
    ) {
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);
        if (inputs.size() != 1) {
            throw new IllegalStateException("There must be one input to job root.");
        }
        Vertex input = sole(inputs);
        RelDataType rowType = rel.getRowType();
        TupleMapping selection = rel.getSelection();
        if (selection != null && selection.size() == rowType.getFieldCount() && selection.isIdentity()) {
            selection = null;
        }
        RootParam param = new RootParam(DefinitionMapper.mapToDingoType(rel.getRowType()), selection);
        Vertex vertex = new Vertex(ROOT, param);
        Task task = input.getTask();
        CommonId id = idGenerator.getOperatorId(task.getId());
        vertex.setId(id);
        Edge edge = new Edge(input, vertex);
        input.addEdge(edge);
        vertex.addIn(edge);
        task.putVertex(vertex);
        task.markRoot(id);
        job.markRoot(task.getId());
        return ImmutableList.of();
    }
}
