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
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.params.PartInsertParam;
import io.dingodb.exec.operator.params.PessimisticLockInsertIgnoreParam;
import io.dingodb.exec.operator.params.PessimisticLockParam;
import io.dingodb.exec.operator.params.TxnPartInsertIgnoreParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_INSERT;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_LOCK;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_LOCK_INSERT_IGNORE;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_INSERT_IGNORE;

public final class DingoInsertIgnoreVisitFun {
    private DingoInsertIgnoreVisitFun() {
    }

    public static Collection<Vertex> visit(Job job, IdGenerator idGenerator, Location currentLocation,
                                           ITransaction transaction, DingoJobVisitor visitor, DingoTableModify rel,
                                           boolean forUpdate, boolean replaceInto, boolean isIgnore, long updateLimit
    ) {
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);
        List<Vertex> outputs = new LinkedList<>();
        final Table td = rel.getTable().unwrap(DingoTable.class).getTable();
        final CommonId tableId = MetaServiceUtils.getTableId(rel.getTable());

        for (Vertex input : inputs) {

            Task task = input.getTask();
            Vertex vertex;
            boolean isScan = visitor.isScan() && !input.getTask().getBachTask();
            switch (rel.getOperation()) {
                case INSERT:
                    if (transaction != null) {
                        boolean pessimisticTxn = transaction.isPessimistic();
                        TupleMapping updateMapping = null;
                        List<SqlExpr> updates = null;
                        boolean isUpdate = false;
                        if ((rel.getTargetColumnNames() != null && !rel.getTargetColumnNames().isEmpty())
                            && !rel.getSourceExpressionList2().isEmpty()
                        ) {
                            List<String> colNames = td.getColumns().stream()
                                .map(Column::getName).map(String::toUpperCase).toList();
                            updateMapping = TupleMapping.of(rel.getTargetColumnNames().stream()
                                .map(String::toUpperCase).map(colNames::indexOf).collect(Collectors.toList()));
                            updates = rel.getSourceExpressionList2()
                                .stream().map(r -> {
                                    if (r == null) {
                                        return null;
                                    } else {
                                        return SqlExprUtils.toSqlExpr(r);
                                    }
                                }).collect(Collectors.toList());
                            isUpdate = true;
                        }
                        if (pessimisticTxn) {
                            Vertex lockVertex ;
                            if (transaction.getPrimaryKeyLock() == null) {
                                PessimisticLockParam pessimisticLockParam = new PessimisticLockParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    transaction.getIsolationLevel(),
                                    transaction.getStartTs(),
                                    transaction.getForUpdateTs(),
                                    true,
                                    transaction.getPrimaryKeyLock(),
                                    transaction.getLockTimeOut(),
                                    true,
                                    isScan,
                                    "insert",
                                    td,
                                    isUpdate,
                                    forUpdate,
                                    replaceInto,
                                    isIgnore,
                                    false,
                                    updateMapping,
                                    updates,
                                    updateLimit,
                                    null
                                );
                                lockVertex = new Vertex(PESSIMISTIC_LOCK, pessimisticLockParam);
                            } else {
                                PessimisticLockInsertIgnoreParam pessimisticLockParam =
                                    new PessimisticLockInsertIgnoreParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    transaction.getIsolationLevel(),
                                    transaction.getStartTs(),
                                    transaction.getForUpdateTs(),
                                    true,
                                    transaction.getPrimaryKeyLock(),
                                    transaction.getLockTimeOut(),
                                    isScan,
                                    td
                                );
                                lockVertex = new Vertex(PESSIMISTIC_LOCK_INSERT_IGNORE, pessimisticLockParam);
                            }
                            lockVertex.setId(idGenerator.getOperatorId(task.getId()));
                            Edge inputEdge = new Edge(input, lockVertex);
                            input.addEdge(inputEdge);
                            lockVertex.addIn(inputEdge);
                            task.putVertex(lockVertex);

                            Vertex insertVertex = new Vertex(TXN_PART_INSERT_IGNORE,
                                new TxnPartInsertIgnoreParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    true,
                                    transaction.getIsolationLevel(),
                                    transaction.getPrimaryKeyLock(),
                                    transaction.getStartTs(),
                                    transaction.getForUpdateTs(),
                                    transaction.getLockTimeOut(),
                                    visitor.getExecuteVariables().isInsertCheckInplace(),
                                    td,
                                    rel.isHasAutoIncrement(),
                                    rel.getAutoIncrementColIndex()
                                )
                            );
                            insertVertex.setId(idGenerator.getOperatorId(task.getId()));
                            Edge lockEdge = new Edge(lockVertex, insertVertex);
                            lockVertex.addEdge(lockEdge);
                            insertVertex.addIn(lockEdge);
                            OutputHint hint = new OutputHint();
                            hint.setToSumUp(true);
                            insertVertex.setHint(hint);
                            task.putVertex(insertVertex);
                            outputs.add(insertVertex);
                        } else {
                            vertex = new Vertex(TXN_PART_INSERT_IGNORE,
                                new TxnPartInsertIgnoreParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    false,
                                    transaction.getIsolationLevel(),
                                    null,
                                    transaction.getStartTs(),
                                    0L,
                                    transaction.getLockTimeOut(),
                                    visitor.getExecuteVariables().isInsertCheckInplace(),
                                    td,
                                    rel.isHasAutoIncrement(),
                                    rel.getAutoIncrementColIndex()
                                )
                            );
                            vertex.setId(idGenerator.getOperatorId(task.getId()));
                            task.putVertex(vertex);
                            input.setPin(0);
                            OutputHint hint = new OutputHint();
                            hint.setToSumUp(true);
                            vertex.setHint(hint);
                            Edge edge = new Edge(input, vertex);
                            input.addEdge(edge);
                            vertex.addIn(edge);
                            outputs.add(vertex);
                        }
                    } else {
                        vertex = new Vertex(
                            PART_INSERT,
                            new PartInsertParam(tableId, td.tupleType(), td.keyMapping(),
                                td, rel.isHasAutoIncrement(), rel.getAutoIncrementColIndex())
                        );
                        vertex.setId(idGenerator.getOperatorId(task.getId()));
                        task.putVertex(vertex);
                        input.setPin(0);
                        OutputHint hint = new OutputHint();
                        hint.setToSumUp(true);
                        vertex.setHint(hint);
                        Edge edge = new Edge(input, vertex);
                        input.addEdge(edge);
                        vertex.addIn(edge);
                        outputs.add(vertex);
                    }
                    break;
                default:
                    throw new IllegalStateException("Operation \"" + rel.getOperation() + "\" is not supported.");
            }
        }
        return outputs;
    }
}
