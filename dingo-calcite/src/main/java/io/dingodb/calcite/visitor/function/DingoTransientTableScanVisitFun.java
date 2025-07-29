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

import io.dingodb.calcite.rel.DingoTransientTableScan;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.ListTransientScanParam;
import io.dingodb.exec.transaction.base.ITransaction;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.dingodb.exec.utils.OperatorCodeUtils.LIST_TRANSIENT_SCAN;


public class DingoTransientTableScanVisitFun {
    public static @NonNull Collection<Vertex> visit(
        Job job,
        @NonNull IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        ITransaction transaction,
        @NonNull DingoTransientTableScan rel
    ) {
        ListTransientScanParam listTransientScanParam = new ListTransientScanParam(rel.getTransientTable());
        Task task = job.getOrCreate(currentLocation, idGenerator);
        Vertex vertex = new Vertex(LIST_TRANSIENT_SCAN, listTransientScanParam);
        vertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(vertex);

        List<Vertex> outputs = new ArrayList<>();
        outputs.add(vertex);
        return outputs;
    }
}
