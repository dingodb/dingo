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
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.EmptySourceParam;
import io.dingodb.exec.operator.params.GetByKeysParam;
import io.dingodb.exec.operator.params.GetDistributionParam;
import io.dingodb.exec.operator.params.TxnGetByKeysParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.tso.TsoService;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.exec.utils.OperatorCodeUtils.EMPTY_SOURCE;
import static io.dingodb.exec.utils.OperatorCodeUtils.GET_BY_KEYS;
import static io.dingodb.exec.utils.OperatorCodeUtils.GET_DISTRIBUTION;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_GET_BY_KEYS;

public final class DingoGetByKeysFun {
    private DingoGetByKeysFun() {
    }

    @NonNull
    public static List<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor,
        ITransaction transaction, @NonNull DingoGetByKeys rel
    ) {
        final TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        final NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions
            = tableInfo.getRangeDistributions();
        final Table td = rel.getTable().unwrap(DingoTable.class).getTable();
        final List<Vertex> outputs = new LinkedList<>();
        List<Object[]> keyTuples = TableUtils.getTuplesForKeyMapping(rel.getPoints(), td);
        if (keyTuples.isEmpty()) {
            Task task = job.getOrCreate(currentLocation, idGenerator);
            EmptySourceParam param = new EmptySourceParam();
            Vertex vertex = new Vertex(EMPTY_SOURCE, param);
            OutputHint hint = new OutputHint();
            hint.setPartId(null);
            vertex.setHint(hint);
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            outputs.add(vertex);
            return outputs;
        }
        GetDistributionParam distributionParam
            = new GetDistributionParam(keyTuples, td.keyMapping(), td, distributions);
        Vertex distributionVertex = new Vertex(GET_DISTRIBUTION, distributionParam);
        Task task;
        if (transaction != null) {
            task = job.getOrCreate(
                currentLocation,
                idGenerator,
                transaction.getType(),
                IsolationLevel.of(transaction.getIsolationLevel())
            );
        } else {
            task = job.getOrCreate(currentLocation, idGenerator);
        }
        distributionVertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(distributionVertex);
        Vertex getVertex;
        long scanTs = Optional.ofNullable(transaction).map(ITransaction::getStartTs).orElse(0L);
        // Use current read
        if (transaction != null && transaction.isPessimistic()
            && IsolationLevel.of(transaction.getIsolationLevel()) == IsolationLevel.SnapshotIsolation
            && (visitor.getKind() == SqlKind.INSERT || visitor.getKind() == SqlKind.DELETE
            || visitor.getKind() == SqlKind.UPDATE) ) {
            scanTs = TsoService.getDefault().tso();
        }
        if (transaction != null && transaction.isPessimistic()
            && IsolationLevel.of(transaction.getIsolationLevel()) == IsolationLevel.ReadCommitted
            && visitor.getKind() == SqlKind.SELECT) {
            scanTs = TsoService.getDefault().tso();
        }
        if (transaction != null) {
            TxnGetByKeysParam param = new TxnGetByKeysParam(
                tableInfo.getId(),
                td.tupleType(),
                td.keyMapping(),
                SqlExprUtils.toSqlExpr(rel.getFilter()),
                rel.getSelection(),
                td,
                scanTs,
                transaction.getIsolationLevel(),
                transaction.getLockTimeOut(),
                visitor.getKind() == SqlKind.SELECT
            );
            getVertex = new Vertex(TXN_GET_BY_KEYS, param);
        } else {
            GetByKeysParam param = new GetByKeysParam(tableInfo.getId(), td.tupleType(),
                td.keyMapping(), SqlExprUtils.toSqlExpr(rel.getFilter()), rel.getSelection(), td
            );
            getVertex = new Vertex(GET_BY_KEYS, param);
        }

        task = job.getOrCreate(currentLocation, idGenerator);
        OutputHint hint = new OutputHint();
        getVertex.setHint(hint);
        getVertex.setId(idGenerator.getOperatorId(task.getId()));
        Edge edge = new Edge(distributionVertex, getVertex);
        distributionVertex.addEdge(edge);
        getVertex.addIn(edge);
        task.putVertex(getVertex);
        outputs.add(getVertex);
        return outputs;
    }
}
