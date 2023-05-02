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

import io.dingodb.calcite.rel.DingoPartRangeDelete;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.PartRangeDeleteOperator;
import io.dingodb.exec.partition.PartitionStrategy;
import io.dingodb.exec.partition.RangeStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public final class DingoRangeDeleteVisitFun {

    private DingoRangeDeleteVisitFun() {
    }

    public static Collection<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, DingoPartRangeDelete rel
    ) {
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        final TableDefinition td = TableUtils.getTableDefinition(rel.getTable());
        List<Output> outputs = new ArrayList<>();

        NavigableMap<ComparableByteArray, RangeDistribution> distributions = tableInfo.getRangeDistributions();
        // Get all partitions based on startKey and endKey
        final PartitionStrategy<CommonId, byte[]> ps = new RangeStrategy(td, distributions);
        byte[] startKey = rel.getStartKey();
        byte[] endKey = rel.getEndKey();
        if (startKey == null) {
            startKey = distributions.firstEntry().getValue().getStartKey();
        }
        if (endKey == null) {
            endKey = distributions.lastEntry().getValue().getEndKey();
        }
        Map<byte[], byte[]> partMap = ps.calcPartitionRange(startKey, endKey, false);
        for (Map.Entry<byte[], byte[]> next : partMap.entrySet()) {
            PartRangeDeleteOperator operator = new PartRangeDeleteOperator(
                tableInfo.getId(),
                distributions.floorEntry(new ComparableByteArray(next.getKey())).getValue().id(),
                td.getDingoType(),
                td.getKeyMapping(),
                next.getKey(),
                next.getValue(),
                rel.isIncludeStart(),
                rel.isIncludeEnd()
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(currentLocation, idGenerator);
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }
}
