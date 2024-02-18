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
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.RangeUtils;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
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
import io.dingodb.exec.operator.params.PartRangeScanParam;
import io.dingodb.exec.operator.params.TxnPartRangeScanParam;
import io.dingodb.exec.transaction.base.ITransaction;
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
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_RANGE_SCAN;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_RANGE_SCAN;

@Slf4j
public final class DingoTableScanVisitFun {

    private DingoTableScanVisitFun() {
    }

    public static @NonNull Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation,
        ITransaction transaction, DingoJobVisitor visitor, @NonNull DingoTableScan rel
    ) {
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        final Table td = rel.getTable().unwrap(DingoTable.class).getTable();

        NavigableMap<ComparableByteArray, RangeDistribution> ranges = tableInfo.getRangeDistributions();
        SqlExpr filter = null;
        byte[] startKey = null;
        byte[] endKey = null;
        boolean withStart = true;
        boolean withEnd = false;
        if (rel.getFilter() != null) {
            filter = SqlExprUtils.toSqlExpr(rel.getFilter());
            KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(td.tupleType(), td.keyMapping());
            RangeDistribution range = RangeUtils.createRangeByFilter(td, codec, rel.getFilter(), rel.getSelection());
            if (range != null) {
                startKey = range.getStartKey();
                endKey = range.getEndKey();
                withStart = range.isWithStart();
                withEnd = range.isWithEnd();
            }
        }
        DistributionSourceParam distributionParam = new DistributionSourceParam(
            td,
            ranges,
            startKey,
            endKey,
            withStart,
            withEnd,
            filter,
            Optional.mapOrGet(rel.getFilter(), __ -> __.getKind() == SqlKind.NOT, () -> false),
            false,
            null);
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
            || visitor.getKind() == SqlKind.UPDATE) ) {
            scanTs = TsoService.getDefault().tso();
        }
        if (transaction != null && transaction.isPessimistic()
            && IsolationLevel.of(transaction.getIsolationLevel()) == IsolationLevel.ReadCommitted
            && visitor.getKind() == SqlKind.SELECT) {
            scanTs = TsoService.getDefault().tso();
        }
        // TODO
        for (int i = 0; i <= Optional.mapOrGet(td.getPartitions(), List::size, () -> 0); i++) {
            Vertex scanVertex;
            if (transaction != null) {
                task = job.getOrCreate(
                    currentLocation,
                    idGenerator,
                    transaction.getType(),
                    IsolationLevel.of(transaction.getIsolationLevel())
                );
                TxnPartRangeScanParam param = new TxnPartRangeScanParam(
                    tableInfo.getId(),
                    td.tupleType(),
                    td.keyMapping(),
                    Optional.mapOrNull(filter, SqlExpr::copy),
                    rel.getSelection(),
                    rel.getGroupSet() == null ? null
                        : AggFactory.getAggKeys(rel.getGroupSet()),
                    rel.getAggCalls() == null ? null : AggFactory.getAggList(
                        rel.getAggCalls(), DefinitionMapper.mapToDingoType(rel.getSelectedType())),
                    DefinitionMapper.mapToDingoType(rel.getNormalRowType()),
                    scanTs,
                    transaction.getIsolationLevel(),
                    transaction.getLockTimeOut(),
                    false
                );
                scanVertex = new Vertex(TXN_PART_RANGE_SCAN, param);
            } else {
                task = job.getOrCreate(currentLocation, idGenerator);
                PartRangeScanParam param = new PartRangeScanParam(
                    tableInfo.getId(),
                    td.tupleType(),
                    td.keyMapping(),
                    Optional.mapOrNull(filter, SqlExpr::copy),
                    rel.getSelection(),
                    rel.getGroupSet() == null ? null
                        : AggFactory.getAggKeys(rel.getGroupSet()),
                    rel.getAggCalls() == null ? null : AggFactory.getAggList(
                        rel.getAggCalls(), DefinitionMapper.mapToDingoType(rel.getSelectedType())),
                    DefinitionMapper.mapToDingoType(rel.getNormalRowType()),
                    rel.isPushDown()
                );
                scanVertex = new Vertex(PART_RANGE_SCAN, param);
            }
            scanVertex.setHint(new OutputHint());
            scanVertex.setId(idGenerator.getOperatorId(task.getId()));
            Edge edge = new Edge(calcVertex, scanVertex);
            calcVertex.addEdge(edge);
            scanVertex.addIn(edge);
            task.putVertex(scanVertex);
            outputs.add(scanVertex);
        }

        return outputs;
    }

}
