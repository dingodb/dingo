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
import io.dingodb.calcite.rel.dingo.IndexFullScan;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.DistributionSourceParam;
import io.dingodb.exec.operator.params.TxnIndexRangeScanParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.tso.TsoService;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.exec.utils.OperatorCodeUtils.CALC_DISTRIBUTION;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_INDEX_RANGE_SCAN;

public final class DingoIndexFullScanVisitFun {

    private DingoIndexFullScanVisitFun() {
    }

    public static @NonNull Collection<Vertex> visit(
        Job job,
        @NonNull IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        ITransaction transaction,
        @NonNull IndexFullScan rel
    ) {
        final LinkedList<Vertex> outputs = new LinkedList<>();
        MetaService metaService = MetaServiceUtils.getMetaService(rel.getTable());
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        final Table td = Objects.requireNonNull(rel.getTable().unwrap(DingoTable.class)).getTable();

        CommonId idxId = rel.getIndexId();
        Table indexTd = rel.getIndexTable();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> indexRanges = metaService
            .getRangeDistribution(idxId);
        DistributionSourceParam distributionParam = new DistributionSourceParam(
            td,
            indexRanges,
            null,
            null,
            true,
            false,
            null,
            Optional.mapOrGet(rel.getFilter(), __ -> __.getKind() == SqlKind.NOT, () -> false),
            false,
            null);
        distributionParam.setKeepOrder(rel.getKeepSerialOrder());
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

        List<Integer> indexSelectionList = indexTd.getColumns().stream().map(td.columns::indexOf).collect(Collectors.toList());
        TupleMapping tupleMapping = TupleMapping.of(
            indexSelectionList
        );

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

        RexNode rexFilter = rel.getFilter();

        RelOp relOp = null;
        Mapping mapping = Mappings.target(indexSelectionList, td.getColumns().size());
        if (rexFilter != null) {
            rexFilter = RexUtil.apply(mapping, rexFilter);
            if (rexFilter != null) {
                Expr expr = RexConverter.convert(rexFilter);
                relOp = RelOpBuilder.builder().filter(expr).build();
            }
        }

        Vertex indexScanvertex = null;
        if (transaction != null) {
            indexScanvertex = new Vertex(TXN_INDEX_RANGE_SCAN, new TxnIndexRangeScanParam(
                idxId,
                tableInfo.getId(),
                tupleMapping,
                DefinitionMapper.mapToDingoType(rel.getRowType()),
                false,
                indexTd,
                td,
                rel.isLookup(),
                scanTs,
                transaction.getLockTimeOut(),
                relOp,
                rel.isPushDown(),
                rel.getSelection(),
                0
            ));
        }
        assert indexScanvertex != null;
        indexScanvertex.setHint(new OutputHint());
        indexScanvertex.setId(idGenerator.getOperatorId(task.getId()));

        Edge edge = new Edge(calcVertex, indexScanvertex);
        calcVertex.addEdge(edge);
        indexScanvertex.addIn(edge);

        task.putVertex(indexScanvertex);
        outputs.add(indexScanvertex);
        return outputs;
    }
}
