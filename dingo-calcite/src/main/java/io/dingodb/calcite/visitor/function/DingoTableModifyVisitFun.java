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
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.PartDeleteParam;
import io.dingodb.exec.operator.params.PartInsertParam;
import io.dingodb.exec.operator.params.PartUpdateParam;
import io.dingodb.exec.operator.params.PessimisticLockDeleteParam;
import io.dingodb.exec.operator.params.PessimisticLockInsertParam;
import io.dingodb.exec.operator.params.PessimisticLockUpdateParam;
import io.dingodb.exec.operator.params.TxnPartDeleteParam;
import io.dingodb.exec.operator.params.TxnPartInsertParam;
import io.dingodb.exec.operator.params.TxnPartUpdateParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.exec.transaction.base.TransactionType.NONE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_INSERT;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_UPDATE;
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
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        final CommonId tableId = MetaServiceUtils.getTableId(rel.getTable());
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions =
            tableInfo.getRangeDistributions();

        for (Vertex input : inputs) {

            Task task = input.getTask();
            Vertex vertex;
            switch (rel.getOperation()) {
                case INSERT:
                    if (transaction != null) {
                        boolean pessimisticTxn = transaction.isPessimistic();
                        if (pessimisticTxn && transaction.getPrimaryKeyLock() == null) {
                            vertex = new Vertex(PESSIMISTIC_LOCK_INSERT,
                                new PessimisticLockInsertParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    transaction.getIsolationLevel(),
                                    transaction.getStartTs(),
                                    transaction.getForUpdateTs(),
                                    true,
                                    transaction.getPrimaryKeyLock(),
                                    transaction.getLockTimeOut(),
                                    td,
                                    distributions));
                        } else {
                            vertex = new Vertex(TXN_PART_INSERT,
                                new TxnPartInsertParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    pessimisticTxn,
                                    transaction.getIsolationLevel(),
                                    pessimisticTxn ? transaction.getPrimaryKeyLock() : null,
                                    transaction.getStartTs(),
                                    pessimisticTxn ? transaction.getForUpdateTs() : 0L,
                                    transaction.getLockTimeOut(),
                                    td,
                                    distributions));
                        }
                    } else {
                        vertex = new Vertex(
                            PART_INSERT,
                            new PartInsertParam(tableId, td.tupleType(), td.keyMapping(), td, distributions)
                        );
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
                        if (pessimisticTxn && transaction.getPrimaryKeyLock() == null) {
                            vertex = new Vertex(PESSIMISTIC_LOCK_UPDATE,
                                new PessimisticLockUpdateParam(
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
                                    td,
                                    distributions
                                )
                            );
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
                                    pessimisticTxn,
                                    transaction.getIsolationLevel(),
                                    pessimisticTxn ? transaction.getPrimaryKeyLock() : null,
                                    transaction.getStartTs(),
                                    pessimisticTxn ? transaction.getForUpdateTs() : 0L,
                                    transaction.getLockTimeOut(),
                                    td,
                                    distributions
                                )
                            );
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
                                distributions
                            )
                        );
                    }
                    break;
                case DELETE:
                    if (transaction != null) {
                        boolean pessimisticTxn = transaction.isPessimistic();
                        if (pessimisticTxn && transaction.getPrimaryKeyLock() == null) {
                            vertex = new Vertex(PESSIMISTIC_LOCK_DELETE,
                                new PessimisticLockDeleteParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    transaction.getIsolationLevel(),
                                    transaction.getStartTs(),
                                    transaction.getForUpdateTs(),
                                    true,
                                    transaction.getPrimaryKeyLock(),
                                    transaction.getLockTimeOut(),
                                    td,
                                    distributions
                                )
                            );
                        } else {
                            vertex = new Vertex(TXN_PART_DELETE,
                                new TxnPartDeleteParam(
                                    tableId,
                                    td.tupleType(),
                                    td.keyMapping(),
                                    pessimisticTxn,
                                    transaction.getIsolationLevel(),
                                    pessimisticTxn ? transaction.getPrimaryKeyLock() : null,
                                    transaction.getStartTs(),
                                    pessimisticTxn ? transaction.getForUpdateTs() : 0L,
                                    transaction.getLockTimeOut(),
                                    td,
                                    distributions
                                )
                            );
                        }
                    } else {
                        vertex = new Vertex(PART_DELETE,
                            new PartDeleteParam(tableId, td.tupleType(),
                                td.keyMapping(), td, distributions)
                        );
                    }
                    break;
                default:
                    throw new IllegalStateException("Operation \"" + rel.getOperation() + "\" is not supported.");
            }
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
        return outputs;
    }
}
