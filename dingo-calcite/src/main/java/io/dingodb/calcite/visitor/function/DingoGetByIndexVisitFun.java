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
import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
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
import io.dingodb.exec.operator.params.DistributionSourceParam;
import io.dingodb.exec.operator.params.GetByIndexParam;
import io.dingodb.exec.operator.params.TxnGetByIndexParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.tso.TsoService;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;

import static io.dingodb.common.util.Utils.calculatePrefixCount;
import static io.dingodb.common.util.Utils.isNeedLookUp;
import static io.dingodb.exec.utils.OperatorCodeUtils.CALC_DISTRIBUTION;
import static io.dingodb.exec.utils.OperatorCodeUtils.GET_BY_INDEX;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_GET_BY_INDEX;

public final class DingoGetByIndexVisitFun {

    private DingoGetByIndexVisitFun() {
    }

    @NonNull
    public static LinkedList<Vertex> visit(
        Job job,
        IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        ITransaction transaction,
        @NonNull DingoGetByIndex rel
    ) {
        final LinkedList<Vertex> outputs = new LinkedList<>();
        MetaService metaService = MetaServiceUtils.getMetaService(rel.getTable());
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        Map<CommonId, Set> indexSetMap = rel.getIndexSetMap();
        final Table td = rel.getTable().unwrap(DingoTable.class).getTable();
        boolean needLookup = false;
        if (indexSetMap.size() > 1) {
            needLookup = true;
        }
        for (Map.Entry<CommonId, Set> indexValSet : indexSetMap.entrySet()) {
            CommonId idxId = indexValSet.getKey();
            Table indexTd = rel.getIndexTdMap().get(idxId);
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> indexRanges = metaService
                .getRangeDistribution(idxId);

            List<Object[]> keyTuples = TableUtils.getTuplesForKeyMapping(indexValSet.getValue(), indexTd);

            KeyValueCodec codec =
                CodecService.getDefault().createKeyValueCodec(indexTd.tupleType(), indexTd.keyMapping());
            List<ByteArrayUtils.ComparableByteArray> keyList = new ArrayList<>();
            for (Object[] keyTuple : keyTuples) {
                byte[] keys = codec.encodeKeyPrefix(keyTuple, calculatePrefixCount(keyTuple));
                if (keyList.contains(new ByteArrayUtils.ComparableByteArray(keys))) {
                    continue;
                }
                keyList.add(new ByteArrayUtils.ComparableByteArray(keys));
                Vertex distributionVertex = new Vertex(CALC_DISTRIBUTION, new DistributionSourceParam(
                    indexTd,
                    indexRanges,
                    keys,
                    keys,
                    true,
                    true,
                    null,
                    false,
                    false,
                    keyTuple));

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

                List<Column> columnNames = indexTd.getColumns();
                TupleMapping tupleMapping = TupleMapping.of(
                    columnNames.stream().map(td.columns::indexOf).collect(Collectors.toList())
                );
                if (!needLookup) {
                    needLookup = isNeedLookUp(rel.getSelection(), tupleMapping, td.columns.size());
                }
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
                Vertex vertex;
                if (transaction != null) {
                    vertex = new Vertex(TXN_GET_BY_INDEX, new TxnGetByIndexParam(
                        idxId,
                        tableInfo.getId(),
                        tupleMapping,
                        SqlExprUtils.toSqlExpr(rel.getFilter()),
                        rel.getSelection(),
                        rel.isUnique(),
                        indexTd,
                        td,
                        needLookup,
                        scanTs,
                        transaction.getLockTimeOut()
                    ));
                } else {
                    vertex = new Vertex(GET_BY_INDEX, new GetByIndexParam(
                        idxId,
                        tableInfo.getId(),
                        tupleMapping,
                        SqlExprUtils.toSqlExpr(rel.getFilter()),
                        rel.getSelection(),
                        rel.isUnique(),
                        indexTd,
                        td,
                        needLookup
                    ));
                }
                OutputHint hint = new OutputHint();
                vertex.setHint(hint);
                vertex.setId(idGenerator.getOperatorId(task.getId()));
                Edge edge = new Edge(distributionVertex, vertex);
                distributionVertex.addEdge(edge);
                vertex.addIn(edge);
                task.putVertex(vertex);
                outputs.add(vertex);
            }

        }
        return outputs;
    }

}
