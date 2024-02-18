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
import io.dingodb.calcite.rel.dingo.DingoScanWithRelOp;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.params.DistributionSourceParam;
import io.dingodb.exec.operator.params.ScanParam;
import io.dingodb.exec.operator.params.ScanWithRelOpParam;
import io.dingodb.exec.operator.params.TxnScanParam;
import io.dingodb.exec.operator.params.TxnScanWithRelOpParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.expr.rel.CacheOp;
import io.dingodb.expr.rel.PipeOp;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.runtime.exception.NeverRunHere;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.exec.utils.OperatorCodeUtils.CALC_DISTRIBUTION;
import static io.dingodb.exec.utils.OperatorCodeUtils.SCAN_WITH_CACHE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.SCAN_WITH_NO_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.SCAN_WITH_PIPE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_SCAN_WITH_CACHE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_SCAN_WITH_NO_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_SCAN_WITH_PIPE_OP;

@Slf4j
public final class DingoScanWithRelOpVisitFun {
    private DingoScanWithRelOpVisitFun() {
    }

    public static @NonNull Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation,
        ITransaction transaction, DingoJobVisitor visitor, @NonNull DingoScanWithRelOp rel
    ) {
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        final Table td = rel.getTable().unwrap(DingoTable.class).getTable();

        NavigableMap<ComparableByteArray, RangeDistribution> ranges = tableInfo.getRangeDistributions();
        SqlExpr filter = null;
        byte[] startKey = null;
        byte[] endKey = null;
        boolean withStart = true;
        boolean withEnd = false;
        // TODO: need to create range by filters.
        DistributionSourceParam distributionParam = new DistributionSourceParam(
            td,
            ranges,
            startKey,
            endKey,
            withStart,
            withEnd,
            filter,
            false,
            false,
            null
        );
        Vertex calcVertex = new Vertex(CALC_DISTRIBUTION, distributionParam);
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
        calcVertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(calcVertex);

        List<Vertex> outputs = new ArrayList<>();

        long scanTs = Optional.ofNullable(transaction).map(ITransaction::getStartTs).orElse(0L);
        // Use current read
        if (transaction != null && transaction.isPessimistic()
            && IsolationLevel.of(transaction.getIsolationLevel()) == IsolationLevel.SnapshotIsolation
            && (visitor.getKind() == SqlKind.INSERT || visitor.getKind() == SqlKind.DELETE
            || visitor.getKind() == SqlKind.UPDATE)) {
            scanTs = TsoService.getDefault().tso();
        }
        if (transaction != null && transaction.isPessimistic()
            && IsolationLevel.of(transaction.getIsolationLevel()) == IsolationLevel.ReadCommitted
            && visitor.getKind() == SqlKind.SELECT) {
            scanTs = TsoService.getDefault().tso();
        }
        Vertex vertex;
        int partitionNums = td.getPartitions().size();
        if (partitionNums == 0) {
            partitionNums = 1;
        }
        for (int i = 0; i < partitionNums; i++) {
            if (transaction != null) {
                task = job.getOrCreate(
                    currentLocation,
                    idGenerator,
                    transaction.getType(),
                    IsolationLevel.of(transaction.getIsolationLevel())
                );
                RelOp relOp = rel.getRelOp();
                if (relOp == null) {
                    TxnScanParam param = new TxnScanParam(
                        tableInfo.getId(),
                        td.tupleType(),
                        td.keyMapping(),
                        scanTs,
                        transaction.getIsolationLevel(),
                        transaction.getLockTimeOut()
                    );
                    vertex = new Vertex(TXN_SCAN_WITH_NO_OP, param);
                } else {
                    TxnScanWithRelOpParam param = new TxnScanWithRelOpParam(
                        tableInfo.getId(),
                        td.tupleType(),
                        td.keyMapping(),
                        scanTs,
                        transaction.getIsolationLevel(),
                        transaction.getLockTimeOut(),
                        relOp,
                        DefinitionMapper.mapToDingoType(rel.getRowType()),
                        rel.isPushDown()
                    );
                    if (relOp instanceof PipeOp) {
                        vertex = new Vertex(TXN_SCAN_WITH_PIPE_OP, param);
                    } else if (relOp instanceof CacheOp) {
                        vertex = new Vertex(TXN_SCAN_WITH_CACHE_OP, param);
                    } else {
                        throw new NeverRunHere();
                    }
                }
            } else {
                task = job.getOrCreate(currentLocation, idGenerator);
                RelOp relOp = rel.getRelOp();
                if (relOp == null) {
                    ScanParam param = new ScanParam(
                        tableInfo.getId(),
                        td.tupleType(),
                        td.keyMapping()
                    );
                    vertex = new Vertex(SCAN_WITH_NO_OP, param);
                } else {
                    ScanWithRelOpParam param = new ScanWithRelOpParam(
                        tableInfo.getId(),
                        td.tupleType(),
                        td.keyMapping(),
                        relOp,
                        DefinitionMapper.mapToDingoType(rel.getRowType()),
                        rel.isPushDown()
                    );
                    if (relOp instanceof PipeOp) {
                        vertex = new Vertex(SCAN_WITH_PIPE_OP, param);
                    } else if (relOp instanceof CacheOp) {
                        vertex = new Vertex(SCAN_WITH_CACHE_OP, param);
                    } else {
                        throw new NeverRunHere();
                    }
                }
            }
            vertex.setHint(new OutputHint());
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            Edge edge = new Edge(calcVertex, vertex);
            calcVertex.addEdge(edge);
            vertex.addIn(edge);
            task.putVertex(vertex);
            outputs.add(vertex);
        }
        return outputs;
    }
}
