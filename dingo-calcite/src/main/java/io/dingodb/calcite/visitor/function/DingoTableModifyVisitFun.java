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
import io.dingodb.exec.operator.PessimisticLockInsertOperator;
import io.dingodb.exec.operator.params.PartDeleteParam;
import io.dingodb.exec.operator.params.PartInsertParam;
import io.dingodb.exec.operator.params.PartUpdateParam;
import io.dingodb.exec.operator.params.PessimisticLockDeleteParam;
import io.dingodb.exec.operator.params.PessimisticLockInsertParam;
import io.dingodb.exec.operator.params.PessimisticLockParam;
import io.dingodb.exec.operator.params.PessimisticLockUpdateParam;
import io.dingodb.exec.operator.params.TxnPartDeleteParam;
import io.dingodb.exec.operator.params.TxnPartInsertParam;
import io.dingodb.exec.operator.params.TxnPartUpdateParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_INSERT;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_UPDATE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_LOCK;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_LOCK_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_LOCK_INSERT;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_LOCK_UPDATE;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_INSERT;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_UPDATE;

public class DingoTableModifyVisitFun {
    public static Collection<Vertex> visit(Job job, IdGenerator idGenerator, Location currentLocation,
                                           ITransaction transaction, DingoJobVisitor visitor, DingoTableModify rel
    ) {
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);
        List<Vertex> outputs = new LinkedList<>();
        final Table td = rel.getTable().unwrap(DingoTable.class).getTable();
        final CommonId tableId = MetaServiceUtils.getTableId(rel.getTable());

        for (Vertex input : inputs) {

            Task task = input.getTask();
            Vertex vertex;
            switch (rel.getOperation()) {
                case INSERT:
                    if (transaction != null) {
                        boolean pessimisticTxn = transaction.isPessimistic();
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
                                    td
                                );
                                lockVertex = new Vertex(PESSIMISTIC_LOCK, pessimisticLockParam);
                            } else {
                                PessimisticLockInsertParam pessimisticLockParam = new PessimisticLockInsertParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    transaction.getIsolationLevel(),
                                    transaction.getStartTs(),
                                    transaction.getForUpdateTs(),
                                    true,
                                    transaction.getPrimaryKeyLock(),
                                    transaction.getLockTimeOut(),
                                    td
                                );
                                lockVertex = new Vertex(PESSIMISTIC_LOCK_INSERT, pessimisticLockParam);
                            }
                            lockVertex.setId(idGenerator.getOperatorId(task.getId()));
                            Edge inputEdge = new Edge(input, lockVertex);
                            input.addEdge(inputEdge);
                            lockVertex.addIn(inputEdge);
                            task.putVertex(lockVertex);

                            Vertex insertVertex = new Vertex(TXN_PART_INSERT,
                                new TxnPartInsertParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    true,
                                    transaction.getIsolationLevel(),
                                    pessimisticTxn ? transaction.getPrimaryKeyLock() : null,
                                    transaction.getStartTs(),
                                    pessimisticTxn ? transaction.getForUpdateTs() : 0L,
                                    transaction.getLockTimeOut(),
                                    td,
                                    rel.isHasAutoIncrement(),
                                    rel.getAutoIncrementColIndex()));
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
                            vertex = new Vertex(TXN_PART_INSERT,
                                new TxnPartInsertParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    false,
                                    transaction.getIsolationLevel(),
                                    pessimisticTxn ? transaction.getPrimaryKeyLock() : null,
                                    transaction.getStartTs(),
                                    pessimisticTxn ? transaction.getForUpdateTs() : 0L,
                                    transaction.getLockTimeOut(),
                                    td,
                                    rel.isHasAutoIncrement(),
                                    rel.getAutoIncrementColIndex()));
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
                case UPDATE:
                    List<String> colNames = td.getColumns().stream()
                        .map(Column::getName).collect(Collectors.toList());
                    TupleMapping updateMapping = TupleMapping.of(
                        rel.getUpdateColumnList().stream().map(colNames::indexOf).collect(Collectors.toList())
                    );
                    if (transaction != null) {
                        boolean pessimisticTxn = transaction.isPessimistic();
                        if (pessimisticTxn) {
                            Vertex lockVertex;
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
                                    false,
                                    td
                                );
                                lockVertex = new Vertex(PESSIMISTIC_LOCK, pessimisticLockParam);
                            } else {
                                PessimisticLockUpdateParam pessimisticLockParam = new PessimisticLockUpdateParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    updateMapping,
                                    rel.getSourceExpressionList().stream()
                                        .map(SqlExprUtils::toSqlExpr)
                                        .collect(Collectors.toList()),
                                    transaction.getIsolationLevel(),
                                    transaction.getStartTs(),
                                    transaction.getForUpdateTs(),
                                    true,
                                    transaction.getPrimaryKeyLock(),
                                    transaction.getLockTimeOut(),
                                    td
                                );
                                lockVertex = new Vertex(PESSIMISTIC_LOCK_UPDATE, pessimisticLockParam);
                            }
                            lockVertex.setId(idGenerator.getOperatorId(task.getId()));
                            Edge inputEdge = new Edge(input, lockVertex);
                            input.addEdge(inputEdge);
                            lockVertex.addIn(inputEdge);
                            task.putVertex(lockVertex);

                            Vertex updateVertex = new Vertex(TXN_PART_UPDATE,
                                new TxnPartUpdateParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    updateMapping,
                                    rel.getSourceExpressionList().stream()
                                        .map(SqlExprUtils::toSqlExpr)
                                        .collect(Collectors.toList()),
                                    true,
                                    transaction.getIsolationLevel(),
                                    pessimisticTxn ? transaction.getPrimaryKeyLock() : null,
                                    transaction.getStartTs(),
                                    pessimisticTxn ? transaction.getForUpdateTs() : 0L,
                                    transaction.getLockTimeOut(),
                                    td,
                                    rel.isHasAutoIncrement(),
                                    rel.getAutoIncrementColIndex()
                                )
                            );
                            updateVertex.setId(idGenerator.getOperatorId(task.getId()));
                            Edge lockEdge = new Edge(lockVertex, updateVertex);
                            lockVertex.addEdge(lockEdge);
                            updateVertex.addIn(lockEdge);
                            OutputHint hint = new OutputHint();
                            hint.setToSumUp(true);
                            updateVertex.setHint(hint);
                            task.putVertex(updateVertex);
                            outputs.add(updateVertex);
                        } else {
                            vertex = new Vertex(TXN_PART_UPDATE,
                                new TxnPartUpdateParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    updateMapping,
                                    rel.getSourceExpressionList().stream()
                                        .map(SqlExprUtils::toSqlExpr)
                                        .collect(Collectors.toList()),
                                    false,
                                    transaction.getIsolationLevel(),
                                    pessimisticTxn ? transaction.getPrimaryKeyLock() : null,
                                    transaction.getStartTs(),
                                    pessimisticTxn ? transaction.getForUpdateTs() : 0L,
                                    transaction.getLockTimeOut(),
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
                        vertex = new Vertex(PART_UPDATE,
                            new PartUpdateParam(
                                tableId,
                                td.tupleType(),
                                td.keyMapping(),
                                updateMapping,
                                rel.getSourceExpressionList().stream()
                                    .map(SqlExprUtils::toSqlExpr)
                                    .collect(Collectors.toList()),
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
                    break;
                case DELETE:
                    if (transaction != null) {
                        boolean pessimisticTxn = transaction.isPessimistic();
                        if (pessimisticTxn) {
                            Vertex lockVertex;
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
                                    false,
                                    td
                                );
                                lockVertex = new Vertex(PESSIMISTIC_LOCK, pessimisticLockParam);
                            } else {
                                PessimisticLockDeleteParam pessimisticLockParam = new PessimisticLockDeleteParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    transaction.getIsolationLevel(),
                                    transaction.getStartTs(),
                                    transaction.getForUpdateTs(),
                                    true,
                                    transaction.getPrimaryKeyLock(),
                                    transaction.getLockTimeOut(),
                                    td
                                );
                                lockVertex = new Vertex(PESSIMISTIC_LOCK_DELETE, pessimisticLockParam);
                            }
                            lockVertex.setId(idGenerator.getOperatorId(task.getId()));
                            Edge inputEdge = new Edge(input, lockVertex);
                            input.addEdge(inputEdge);
                            lockVertex.addIn(inputEdge);
                            task.putVertex(lockVertex);

                            Vertex delateVertex = new Vertex(TXN_PART_DELETE,
                                new TxnPartDeleteParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    true,
                                    transaction.getIsolationLevel(),
                                    pessimisticTxn ? transaction.getPrimaryKeyLock() : null,
                                    transaction.getStartTs(),
                                    pessimisticTxn ? transaction.getForUpdateTs() : 0L,
                                    transaction.getLockTimeOut(),
                                    td
                                )
                            );
                            delateVertex.setId(idGenerator.getOperatorId(task.getId()));
                            Edge lockEdge = new Edge(lockVertex, delateVertex);
                            lockVertex.addEdge(lockEdge);
                            delateVertex.addIn(lockEdge);
                            OutputHint hint = new OutputHint();
                            hint.setToSumUp(true);
                            delateVertex.setHint(hint);
                            task.putVertex(delateVertex);
                            outputs.add(delateVertex);
                        } else {
                            vertex = new Vertex(TXN_PART_DELETE,
                                new TxnPartDeleteParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    false,
                                    transaction.getIsolationLevel(),
                                    pessimisticTxn ? transaction.getPrimaryKeyLock() : null,
                                    transaction.getStartTs(),
                                    pessimisticTxn ? transaction.getForUpdateTs() : 0L,
                                    transaction.getLockTimeOut(),
                                    td
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
                        vertex = new Vertex(PART_DELETE,
                            new PartDeleteParam(tableId, td.tupleType(), td.keyMapping(), td)
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
