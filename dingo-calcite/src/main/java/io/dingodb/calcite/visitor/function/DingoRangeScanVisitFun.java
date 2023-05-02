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

import io.dingodb.calcite.rel.DingoPartRangeScan;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.PartRangeScanOperator;
import io.dingodb.exec.partition.PartitionStrategy;
import io.dingodb.exec.partition.RangeStrategy;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

@Slf4j
public final class DingoRangeScanVisitFun {

    private DingoRangeScanVisitFun() {
    }

    public static Collection<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, DingoPartRangeScan rel
    ) {
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        final TableDefinition td = TableUtils.getTableDefinition(rel.getTable());
        SqlExpr filter = null;
        if (rel.getFilter() != null) {
            filter = SqlExprUtils.toSqlExpr(rel.getFilter());
        }

        NavigableMap<ComparableByteArray, RangeDistribution> distributions = tableInfo.getRangeDistributions();
        byte[] startKey = rel.getStartKey();
        byte[] endKey = rel.getEndKey();
        if (startKey == null) {
            startKey = distributions.firstEntry().getValue().getStartKey();
        }
        if (endKey == null) {
            endKey = distributions.lastEntry().getValue().getEndKey();
        }

        ComparableByteArray startByteArray = distributions.floorKey(new ComparableByteArray(startKey));
        if (!rel.isNotBetween() && startByteArray == null) {
            log.warn("Get part from table:{} by startKey:{}, result is null", td.getName(), startKey);
            return null;
        }

        // Get all ranges that need to be queried
        Map<byte[], byte[]> allRangeMap = new TreeMap<>(ByteArrayUtils::compare);
        if (rel.isNotBetween()) {
            allRangeMap.put(distributions.firstKey().getBytes(), startKey);
            allRangeMap.put(endKey, distributions.lastKey().getBytes());
        } else {
            allRangeMap.put(startKey, endKey);
        }

        List<Output> outputs = new ArrayList<Output>();

        Iterator<Map.Entry<byte[], byte[]>> allRangeIterator = allRangeMap.entrySet().iterator();
        while (allRangeIterator.hasNext()) {
            Map.Entry<byte[], byte[]> entry = allRangeIterator.next();
            startKey = entry.getKey();
            endKey = entry.getValue();

            // Get all partitions based on startKey and endKey
            final PartitionStrategy<CommonId, byte[]> ps = new RangeStrategy(td, distributions);
            Map<byte[], byte[]> partMap = ps.calcPartitionRange(startKey, endKey, rel.isIncludeEnd());

            Iterator<Map.Entry<byte[], byte[]>> partIterator = partMap.entrySet().iterator();
            boolean includeStart = rel.isIncludeStart();
            while (partIterator.hasNext()) {
                Map.Entry<byte[], byte[]> next = partIterator.next();
                PartRangeScanOperator operator = new PartRangeScanOperator(
                    tableInfo.getId(),
                    distributions.floorEntry(new ComparableByteArray(next.getKey())).getValue().id(),
                    td.getDingoType(), td.getKeyMapping(), filter, rel.getSelection(), next.getKey(), next.getValue(),
                    includeStart, next.getValue() != null && rel.isIncludeEnd()
                );
                operator.setId(idGenerator.get());
                Task task = job.getOrCreate(currentLocation, idGenerator);
                task.putOperator(operator);
                outputs.addAll(operator.getOutputs());
                includeStart = true;
            }
        }

        return outputs;
    }
}
