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
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.GetByIndexOperator;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.meta.MetaService;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;

import static io.dingodb.common.util.Utils.calculatePrefixCount;

public final class DingoGetByIndexVisitFun {

    public DingoGetByIndexVisitFun() {
    }

    @NonNull
    public static LinkedList<Output> visit(
        Job job,
        IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        @NonNull DingoGetByIndex rel
    ) {
        final LinkedList<Output> outputs = new LinkedList<>();
        MetaService metaService = MetaServiceUtils.getMetaService(rel.getTable());
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        Map<CommonId, Set> indexSetMap = rel.getIndexSetMap();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges = tableInfo.getRangeDistributions();
        final TableDefinition td = TableUtils.getTableDefinition(rel.getTable());
        boolean needLookup = false;
        if (indexSetMap.size() > 1) {
            needLookup = true;
        }
        for (Map.Entry<CommonId, Set> indexValSet : indexSetMap.entrySet()) {
            TableDefinition indexTd = rel.getIndexTdMap().get(indexValSet.getKey());
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> indexRanges
                = metaService.getIndexRangeDistribution(indexValSet.getKey(),
                indexTd);

            PartitionService ps =PartitionService.getService(
                Optional.ofNullable(indexTd.getPartDefinition())
                    .map(PartitionDefinition::getFuncName)
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));

            KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(indexTd.getColumns());
            List<Object[]> keyTuples = TableUtils.getTuplesForKeyMapping(indexValSet.getValue(), indexTd);

            Map<CommonId, List<Object[]>> partMap = new LinkedHashMap<>();
            try {
                for (Object[] tuple : keyTuples) {
                    byte[] keys = codec.encodeKeyPrefix(tuple, calculatePrefixCount(tuple));
                    CommonId partId = ps.calcPartId(keys, indexRanges);
                    partMap.putIfAbsent(partId, new LinkedList<>());
                    partMap.get(partId).add(tuple);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            List<String> columnNames = indexTd.getColumns()
                .stream().map(ColumnDefinition::getName).collect(Collectors.toList());
            TupleMapping tupleMapping = TupleMapping.of(td.getColumnIndices(columnNames));

            if (!needLookup) {
                needLookup = isNeedLookUp(rel.getSelection(), tupleMapping);
            }
            for (Map.Entry<CommonId, List<Object[]>> entry : partMap.entrySet()) {
                GetByIndexOperator operator = new GetByIndexOperator(
                    indexValSet.getKey(),
                    entry.getKey(),
                    tableInfo.getId(),
                    tupleMapping,
                    entry.getValue(),
                    SqlExprUtils.toSqlExpr(rel.getFilter()),
                    rel.getSelection(),
                    rel.isUnique(),
                    ranges,
                    codec,
                    indexTd,
                    td,
                    needLookup
                );
                Task task = job.getOrCreate(currentLocation, idGenerator);
                operator.setId(idGenerator.getOperatorId(task.getId()));
                task.putOperator(operator);
                outputs.addAll(operator.getOutputs());
            }
        }
        return outputs;
    }

    private static boolean isNeedLookUp(TupleMapping selection, TupleMapping keyMapping) {
        if (selection == null) {
            return true;
        }
        for (int index : selection.getMappings()) {
            if (!keyMapping.contains(index)) {
                return true;
            }
        }
        return false;
    }

}
