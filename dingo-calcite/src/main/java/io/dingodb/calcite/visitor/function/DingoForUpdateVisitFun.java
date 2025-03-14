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

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoForUpdate;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.ForUpdateParam;
import io.dingodb.exec.operator.params.PessimisticLockParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.meta.entity.Table;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.common.util.Utils.sole;
import static io.dingodb.exec.utils.OperatorCodeUtils.FOR_UPDATE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_LOCK;

public final class DingoForUpdateVisitFun {

    private DingoForUpdateVisitFun() {
    }

    public static Collection<Vertex> visit(
        Job job,
        IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        ITransaction transaction,
        @NonNull DingoForUpdate rel,
        boolean forUpdate
    ) {
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);

        List<Vertex> outputs = new LinkedList<>();
        Vertex input = sole(inputs);
        Task task = input.getTask();
        if (forUpdate && transaction.isPessimistic()) {
            Table table = rel.getTable().unwrap(DingoTable.class).getTable();
            CommonId tableId = table.tableId;
            boolean isScan = visitor.isScan() && !task.getBachTask();
            Vertex lockVertex;
            if (transaction.getPrimaryKeyLock() == null) {
                PessimisticLockParam param = new PessimisticLockParam(
                    tableId,
                    table.tupleType(),
                    table.keyMapping(),
                    transaction.getIsolationLevel(),
                    transaction.getStartTs(),
                    transaction.getForUpdateTs(),
                    true,
                    transaction.getPrimaryKeyLock(),
                    transaction.getLockTimeOut(),
                    false,
                    isScan,
                    "select",
                    table,
                    false,
                    forUpdate,
                    -1
                );
                lockVertex = new Vertex(PESSIMISTIC_LOCK, param);
            } else {
                ForUpdateParam param = new ForUpdateParam(
                    tableId,
                    table.tupleType(),
                    transaction.getPrimaryKeyLock(),
                    transaction.getStartTs(),
                    transaction.getForUpdateTs(),
                    transaction.getIsolationLevel(),
                    transaction.getLockTimeOut(),
                    isScan,
                    table);
                lockVertex = new Vertex(FOR_UPDATE, param);
            }
            lockVertex.setId(idGenerator.getOperatorId(task.getId()));
            Edge edge = new Edge(input, lockVertex);
            input.addEdge(edge);
            lockVertex.addIn(edge);
            task.putVertex(lockVertex);
            outputs.add(lockVertex);
            return outputs;
        } else {
            return inputs;
        }
    }
}
