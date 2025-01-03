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

import io.dingodb.calcite.rel.dingo.DingoIndexScanWithRelOp;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.utils.VisitUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
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
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Partition;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static io.dingodb.exec.utils.OperatorCodeUtils.CALC_DISTRIBUTION_1;
import static io.dingodb.exec.utils.OperatorCodeUtils.SCAN_WITH_CACHE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.SCAN_WITH_NO_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.SCAN_WITH_PIPE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_SCAN_WITH_CACHE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_SCAN_WITH_NO_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_SCAN_WITH_PIPE_OP;

@Slf4j
public final class DingoIndexScanWithRelOpVisitFun {
    private DingoIndexScanWithRelOpVisitFun() {
    }

    public static @NonNull Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation,
        ITransaction transaction, DingoJobVisitor visitor, @NonNull DingoIndexScanWithRelOp rel
    ) {
        Task task;
        Supplier<Vertex> scanVertexCreator;
        if (transaction != null) {
            task = job.getOrCreate(
                currentLocation,
                idGenerator,
                transaction.getType(),
                IsolationLevel.of(transaction.getIsolationLevel())
            );
            final long scanTs = VisitUtils.getScanTs(transaction, visitor.getKind(), visitor.getPointTs());
            scanVertexCreator = () -> createTxnScanVertex(rel, transaction, scanTs);
        } else {
            task = job.getOrCreate(currentLocation, idGenerator);
            scanVertexCreator = () -> createScanVertex(rel);
        }
        final List<Vertex> outputs = new ArrayList<>();
        final Table td = rel.getIndexTable();
        List<Partition> partitions = td.getPartitions();
        if (partitions.isEmpty()) {
            outputs.add(createVerticesForRange(
                task,
                idGenerator,
                (start, end) -> createCalcRangeDistributionVertex(rel, start, end, false, visitor),
                null,
                null,
                scanVertexCreator
            ));
        } else {
            if (td.getPartitionStrategy().equalsIgnoreCase("HASH")) {
                // Partition will be split in executing time.
                outputs.add(createVerticesForRange(
                    task,
                    idGenerator,
                    (start, end) -> createCalcDistributionVertex(rel, start, end, false, visitor),
                    null,
                    null,
                    scanVertexCreator
                ));
            } else {
                if (rel.getRangeDistribution() != null || !Utils.parallel(rel.getKeepSerialOrder())) {
                    outputs.add(createVerticesForRange(
                        task,
                        idGenerator,
                        (start, end) -> createCalcRangeDistributionVertex(rel, start, end, false, visitor),
                        null,
                        null,
                        scanVertexCreator
                    ));
                    return outputs;
                }
                int partitionNum = partitions.size();
                for (int i = 0; i < partitionNum; ++i) {
                    Partition partition = partitions.get(i);

                    outputs.add(createVerticesForRange(
                        task,
                        idGenerator,
                        (start, end) -> createCalcDistributionVertex(rel, start, end, false, visitor),
                        partition.getStart(),
                        i < partitionNum - 1 ? partitions.get(i + 1).getStart() : null,
                        scanVertexCreator
                    ));
                }
            }
        }
        return outputs;
    }

    private static @NonNull Vertex createVerticesForRange(
        @NonNull Task task,
        @NonNull IdGenerator idGenerator,
        @NonNull BiFunction<byte[], byte[], Vertex> calcVertexCreator,
        byte[] start,
        byte[] end,
        @NonNull Supplier<Vertex> scanVertexCreator
    ) {
        Vertex calcVertex = calcVertexCreator.apply(start, end);
        calcVertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(calcVertex);
        Vertex vertex = scanVertexCreator.get();
        // vertex.setHint(new OutputHint());
        vertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(vertex);
        Edge edge = new Edge(calcVertex, vertex);
        calcVertex.addEdge(edge);
        vertex.addIn(edge);
        return vertex;
    }

    private static @NonNull Vertex createScanVertex(
        @NonNull DingoIndexScanWithRelOp rel
    ) {
        final Table td = rel.getIndexTable();
        RelOp relOp = rel.getRelOp();
        if (relOp == null) {
            ScanParam param = new ScanParam(
                td.tableId,
                td.tupleType(),
                td.keyMapping(),
                td.version,
                td.getCodecVersion()
            );
            return new Vertex(SCAN_WITH_NO_OP, param);
        } else {
            ScanWithRelOpParam param = new ScanWithRelOpParam(
                td.tableId,
                td.tupleType(),
                td.keyMapping(),
                relOp,
                DefinitionMapper.mapToDingoType(rel.getRowType()),
                rel.isPushDown(),
                td.version,
                0,
                td.getCodecVersion()
            );
            if (relOp instanceof PipeOp) {
                return new Vertex(SCAN_WITH_PIPE_OP, param);
            } else if (relOp instanceof CacheOp) {
                return new Vertex(SCAN_WITH_CACHE_OP, param);
            }
        }
        throw new NeverRunHere();
    }

    private static @NonNull Vertex createTxnScanVertex(
        @NonNull DingoIndexScanWithRelOp rel,
        ITransaction transaction,
        long scanTs
    ) {
        final Table td = rel.getIndexTable();
        RelOp relOp = rel.getRelOp();
        if (relOp == null) {
            TxnScanParam param = new TxnScanParam(
                rel.getIndexTable().tableId,
                td.tupleType(),
                td.keyMapping(),
                scanTs,
                transaction.getIsolationLevel(),
                transaction.getLockTimeOut(),
                td.version,
                td.getCodecVersion()
            );
            return new Vertex(TXN_SCAN_WITH_NO_OP, param);
        } else {
            TxnScanWithRelOpParam param = new TxnScanWithRelOpParam(
                rel.getIndexTable().tableId,
                td.tupleType(),
                td.keyMapping(),
                scanTs,
                transaction.getIsolationLevel(),
                transaction.getLockTimeOut(),
                relOp,
                DefinitionMapper.mapToDingoType(rel.getRowType()),
                rel.isPushDown(),
                td.version,
                0,
                td.getCodecVersion()
            );
            if (relOp instanceof PipeOp) {
                return new Vertex(TXN_SCAN_WITH_PIPE_OP, param);
            } else if (relOp instanceof CacheOp) {
                return new Vertex(TXN_SCAN_WITH_CACHE_OP, param);
            }
        }
        throw new NeverRunHere();
    }

    private static @NonNull Vertex createCalcDistributionVertex(
        @NonNull DingoIndexScanWithRelOp rel,
        byte[] startKey,
        byte[] endKey,
        boolean withEnd,
        DingoJobVisitor visitor
    ) {
        MetaService metaService = MetaService.root(visitor.getPointTs());
        final IndexTable td = rel.getIndexTable();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges = metaService
            .getRangeDistribution(td.tableId);

        SqlExpr filter = null;

        if (rel.getFilter() != null) {
            filter = SqlExprUtils.toSqlExpr(rel.getFilter());
        }

        DistributionSourceParam distributionParam = new DistributionSourceParam(
            td,
            ranges,
            startKey,
            endKey,
            true,
            withEnd,
            filter,
            Optional.mapOrGet(rel.getFilter(), __ -> __.getKind() == SqlKind.NOT, () -> false),
            false,
            null,
            visitor.getExecuteVariables().getConcurrencyLevel()
        );
        distributionParam.setKeepOrder(rel.getKeepSerialOrder());
        distributionParam.setFilterRange(rel.isRangeScan());
        return new Vertex(CALC_DISTRIBUTION_1, distributionParam);
    }

    private static @NonNull Vertex createCalcRangeDistributionVertex(
        @NonNull DingoIndexScanWithRelOp rel,
        byte[] startKey,
        byte[] endKey,
        boolean withEnd,
        DingoJobVisitor visitor
    ) {
        MetaService metaService = MetaService.root(visitor.getPointTs());
        final IndexTable td = rel.getIndexTable();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges = metaService
            .getRangeDistribution(td.tableId);

        SqlExpr filter = null;
        boolean withStart = true;

        if (rel.getFilter() != null) {
            filter = SqlExprUtils.toSqlExpr(rel.getFilter());
        }
        if (rel.getRangeDistribution() != null) {
            startKey = rel.getRangeDistribution().getStartKey();
            endKey = rel.getRangeDistribution().getEndKey();
            withStart = rel.getRangeDistribution().isWithStart();
            withEnd = rel.getRangeDistribution().isWithEnd();
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
            null,
            visitor.getExecuteVariables().getConcurrencyLevel()
        );
        boolean filterRange = false;
        distributionParam.setKeepOrder(rel.getKeepSerialOrder());
        distributionParam.setFilterRange(filterRange);
        return new Vertex(CALC_DISTRIBUTION_1, distributionParam);
    }
}
