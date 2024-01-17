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

import io.dingodb.common.Location;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.params.PreWriteParam;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.exec.transaction.visitor.data.PreWriteLeaf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.dingodb.exec.utils.OperatorCodeUtils.PRE_WRITE;

public class DingoPreWriteVisitFun {
    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation,
        ITransaction transaction, DingoTransactionRenderJob visitor, PreWriteLeaf preWriteLeaf) {
        Collection<Vertex> inputs = preWriteLeaf.getData().accept(visitor);
        List<Vertex> outputs = new ArrayList<>();
        for (Vertex input : inputs) {
            PreWriteParam param = new PreWriteParam(
                new BooleanType(true), transaction.getPrimaryKey(), transaction.getStartTs(),
                transaction.getIsolationLevel(), transaction.getType(), transaction.getLockTimeOut()
            );
            Vertex vertex = new Vertex(PRE_WRITE, param);
            Task task = input.getTask();
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            input.setPin(0);
            vertex.setHint(input.getHint());
            Edge edge = new Edge(input, vertex);
            input.addEdge(edge);
            vertex.addIn(edge);
            outputs.add(vertex);
        }
        return outputs;
    }
}
