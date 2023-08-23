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

import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.RangeUtils;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.PartRangeScanOperator;
import io.dingodb.exec.partition.DingoPartitionStrategyFactory;
import io.dingodb.exec.partition.PartitionStrategy;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeSet;

@Slf4j
public final class DingoTableScanVisitFun {

    private DingoTableScanVisitFun() {
    }

    public static @NonNull Collection<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, @NonNull DingoTableScan rel
    ) {
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        final TableDefinition td = TableUtils.getTableDefinition(rel.getTable());

        NavigableSet<RangeDistribution> distributions;
        NavigableMap<ComparableByteArray, RangeDistribution> ranges = tableInfo.getRangeDistributions();
        PartitionStrategy<CommonId, byte[]> ps = DingoPartitionStrategyFactory.createPartitionStrategy(td, ranges);

        SqlExpr filter = null;
        byte[] startKey = null;
        byte[] endKey = null;
        boolean withStart = true;
        boolean withEnd = false;
        if (rel.getFilter() != null) {
            filter = SqlExprUtils.toSqlExpr(rel.getFilter());
            KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(td);
            RangeDistribution range = RangeUtils.createRangeByFilter(td, codec, rel.getFilter(), rel.getSelection());
            if (range != null) {
                startKey = range.getStartKey();
                endKey = range.getEndKey();
                withStart = range.isWithStart();
                withEnd = range.isWithEnd();
            }
            if (rel.getFilter().getKind() == SqlKind.NOT) {
                distributions = new TreeSet<>(io.dingodb.common.util.RangeUtils.rangeComparator());
                distributions.addAll(ps.calcPartitionRange(null, startKey, true, !withStart));
                distributions.addAll(ps.calcPartitionRange(endKey, null, !withEnd, true));
            } else {
                distributions = ps.calcPartitionRange(startKey, endKey, withStart, withEnd);
            }
        } else {
            distributions = ps.calcPartitionRange(startKey, endKey, withStart, withEnd);
        }

        List<Output> outputs = new ArrayList<>();

        for (RangeDistribution rd : distributions) {
            PartRangeScanOperator operator = new PartRangeScanOperator(
                tableInfo.getId(),
                rd.id(),
                td.getDingoType(),
                td.getKeyMapping(),
                Optional.mapOrNull(filter, SqlExpr::copy),
                rel.getSelection(),
                rd.getStartKey(),
                rd.getEndKey(),
                rd.isWithStart(),
                rd.isWithEnd(),
                rel.getGroupSet() == null ? null
                    : AggFactory.getAggKeys(rel.getGroupSet()),
                rel.getAggCalls() == null ? null
                    : AggFactory.getAggList(rel.getAggCalls(), DefinitionMapper.mapToDingoType(rel.getSelectedType())),
                DefinitionMapper.mapToDingoType(rel.getRowType()),
                rel.isPushDown()
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(currentLocation, idGenerator);
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }

        return outputs;
    }
}
