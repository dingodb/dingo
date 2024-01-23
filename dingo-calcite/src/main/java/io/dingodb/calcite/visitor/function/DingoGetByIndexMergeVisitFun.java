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
import io.dingodb.calcite.rel.DingoGetByIndexMerge;
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
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.GetByIndexParam;
import io.dingodb.exec.operator.params.IndexMergeParam;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import lombok.AllArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Supplier;

import static io.dingodb.common.util.Utils.calculatePrefixCount;
import static io.dingodb.exec.utils.OperatorCodeUtils.GET_BY_INDEX;
import static io.dingodb.exec.utils.OperatorCodeUtils.INDEX_MERGE;

public final class DingoGetByIndexMergeVisitFun {
    @NonNull
    public static Collection<Vertex> visit(
        Job job,
        IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        @NonNull DingoGetByIndexMerge rel
    ) {
        //List<Output> outputs = DingoGetByIndexVisitFun.visit(job, idGenerator, currentLocation, visitor, rel);
        //List<Output> inputs = DingoCoalesce.coalesce(idGenerator, outputs);
        //return DingoBridge.bridge(idGenerator, inputs, new DingoGetByIndexMergeVisitFun.OperatorSupplier(rel));
        final LinkedList<Vertex> outputs = new LinkedList<>();
        MetaService metaService = MetaServiceUtils.getMetaService(rel.getTable());
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        Map<CommonId, Set> indexSetMap = rel.getIndexSetMap();
        NavigableMap<ComparableByteArray, RangeDistribution> ranges = tableInfo.getRangeDistributions();
        final Table td = rel.getTable().unwrap(DingoTable.class).getTable();
        boolean needLookup = true;
        for (Map.Entry<CommonId, Set> indexValSet : indexSetMap.entrySet()) {
            CommonId idxId = indexValSet.getKey();
            Table indexTd = rel.getIndexTdMap().get(idxId);
            NavigableMap<ComparableByteArray, RangeDistribution> indexRanges = metaService.getRangeDistribution(idxId);

            PartitionService lookupPs = PartitionService.getService(
                Optional.ofNullable(indexTd.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
            KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(
                indexTd.tupleType(), indexTd.keyMapping())
                ;
            List<Object[]> keyTuples = TableUtils.getTuplesForKeyMapping(indexValSet.getValue(), indexTd);

            Map<CommonId, List<Object[]>> partMap = new LinkedHashMap<>();
            for (Object[] tuple : keyTuples) {
                byte[] keys = codec.encodeKeyPrefix(tuple, calculatePrefixCount(tuple));
                CommonId partId = lookupPs.calcPartId(keys, indexRanges);
                partMap.putIfAbsent(partId, new LinkedList<>());
                partMap.get(partId).add(tuple);
            }

            TupleMapping tupleMapping = indexTd.mapping();
            TupleMapping lookupKeyMapping = indexMergeMapping(td.keyMapping(), rel.getSelection());

            for (Map.Entry<CommonId, List<Object[]>> entry : partMap.entrySet()) {
                GetByIndexParam param = new GetByIndexParam(
                    idxId,
                    entry.getKey(),
                    tableInfo.getId(),
                    tupleMapping,
                    entry.getValue(),
                    SqlExprUtils.toSqlExpr(rel.getFilter()),
                    lookupKeyMapping,
                    rel.isUnique(),
                    ranges,
                    codec,
                    indexTd,
                    td,
                    needLookup
                );
                Task task = job.getOrCreate(currentLocation, idGenerator);
                Vertex vertex = new Vertex(GET_BY_INDEX, param);
                OutputHint hint = new OutputHint();
                hint.setPartId(entry.getKey());
                vertex.setHint(hint);
                vertex.setId(idGenerator.getOperatorId(task.getId()));
                task.putVertex(vertex);
                outputs.add(vertex);
            }
        }

        List<Vertex> inputs = DingoCoalesce.coalesce(idGenerator, outputs);
        return DingoBridge.bridge(idGenerator, inputs, new DingoGetByIndexMergeVisitFun.OperatorSupplier(rel));
    }

    @AllArgsConstructor
    static class OperatorSupplier implements Supplier<Vertex> {

        final DingoGetByIndexMerge relNode;

        @Override
        public Vertex get() {
            IndexMergeParam params = new IndexMergeParam(relNode.getKeyMapping(), relNode.getSelection());
            return new Vertex(INDEX_MERGE, params);
        }
    }

    private static TupleMapping indexMergeMapping(TupleMapping keyMapping, TupleMapping selection) {
        List<Integer> mappings = new ArrayList<>();
        for (int i : selection.getMappings()) {
            mappings.add(i);
        }

        for (int i : keyMapping.getMappings()) {
            if (!mappings.contains(i)) {
                mappings.add(i);
            }
        }
        return TupleMapping.of(mappings);
    }

}