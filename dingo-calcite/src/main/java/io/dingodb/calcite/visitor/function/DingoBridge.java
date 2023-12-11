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

import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

public final class DingoBridge {
    private DingoBridge() {
    }

    public static @NonNull Collection<Vertex> bridge(
        IdGenerator idGenerator, @NonNull Collection<Vertex> inputs, Supplier<Vertex> operatorSupplier
    ) {
        List<Vertex> outputs = new LinkedList<>();

        for (Vertex input : inputs) {
            Vertex vertex = operatorSupplier.get();
            Task task = input.getTask();
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            Edge edge = new Edge(input, vertex);
            vertex.setHint(input.getHint());
            input.addEdge(edge);
            vertex.addIn(edge);
            outputs.add(vertex);
        }
        return outputs;
    }
}
