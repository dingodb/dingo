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
import io.dingodb.calcite.rel.dingo.IndexRangeScan;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.RangeUtils;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.VisitUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
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
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.params.DistributionSourceParam;
import io.dingodb.exec.operator.params.TxnIndexRangeScanParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.data.IsolationLevel;
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

import static io.dingodb.exec.utils.OperatorCodeUtils.CALC_DISTRIBUTION_1;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_INDEX_RANGE_SCAN;

public final class DingoIndexRangeScanVisitFun {
    private DingoIndexRangeScanVisitFun() {
    }

    public static @NonNull Collection<Vertex> visit(
        Job job,
        @NonNull IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        ITransaction transaction,
        @NonNull IndexRangeScan rel
    ) {
        final LinkedList<Vertex> outputs = new LinkedList<>();
        MetaService metaService = MetaService.root(visitor.getPointTs());
        final Table td = Objects.requireNonNull(rel.getTable().unwrap(DingoTable.class)).getTable();

        CommonId idxId = rel.getIndexId();
        Table indexTd = rel.getIndexTable();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> indexRanges = metaService
            .getRangeDistribution(idxId);

        List<Column> columnNames = indexTd.getColumns();
        List<Integer> indexSelectionList = columnNames.stream().map(td.columns::indexOf).collect(Collectors.toList());

        RexNode rexFilter = rel.getFilter();

        RelOp relOp = null;

        Mapping mapping = Mappings.target(indexSelectionList, td.getColumns().size());
        if (rexFilter != null) {
            rexFilter = RexUtil.apply(mapping, rexFilter);
            if (rexFilter != null) {
                Expr expr = RexConverter.convert(rexFilter);
                relOp = RelOpBuilder.builder()
                    .filter(expr)
                    .build();
            }
        }
        SqlExpr filter = null;
        byte[] startKey = null;
        byte[] endKey = null;
        boolean withStart = true;
        boolean withEnd = false;
        if (rexFilter != null) {
            filter = SqlExprUtils.toSqlExpr(rexFilter);
            KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(
                indexTd.getCodecVersion(), indexTd.version, indexTd.tupleType(), indexTd.keyMapping()
            );
            RangeDistribution range = RangeUtils.createRangeByFilter(indexTd, codec, rexFilter, null);
            if (range != null) {
                startKey = range.getStartKey();
                endKey = range.getEndKey();
                withStart = range.isWithStart();
                withEnd = range.isWithEnd();
            }
        }
        DistributionSourceParam distributionParam = new DistributionSourceParam(
            indexTd,
            indexRanges,
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
        distributionParam.setKeepOrder(rel.getKeepSerialOrder());
        Vertex calcVertex = new Vertex(CALC_DISTRIBUTION_1, distributionParam);

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

        Vertex indexScanvertex = null;
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(visitor.getPointTs(), rel.getTable());
        TupleMapping tupleMapping = TupleMapping.of(
            indexSelectionList
        );
        long scanTs = VisitUtils.getScanTs(transaction, visitor.getKind(), visitor.getPointTs());
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
        OutputHint hint = new OutputHint();
        assert indexScanvertex != null;
        indexScanvertex.setHint(hint);
        indexScanvertex.setId(idGenerator.getOperatorId(task.getId()));

        Edge edge = new Edge(calcVertex, indexScanvertex);
        calcVertex.addEdge(edge);
        indexScanvertex.addIn(edge);

        task.putVertex(indexScanvertex);
        outputs.add(indexScanvertex);
        visitor.setScan(true);
        return outputs;
    }
}
