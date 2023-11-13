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
import io.dingodb.common.Location;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.RangeUtils;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.PartRangeDeleteOperator;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeSet;

public final class DingoRangeDeleteVisitFun {

    private DingoRangeDeleteVisitFun() {
    }

    public static Collection<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, DingoPartRangeDelete rel
    ) {
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        final TableDefinition td = TableUtils.getTableDefinition(rel.getTable());
        NavigableSet<RangeDistribution> distributions;
        NavigableMap<ComparableByteArray, RangeDistribution> ranges = tableInfo.getRangeDistributions();
        final PartitionService ps = PartitionService.getService(
            Optional.ofNullable(td.getPartDefinition())
                .map(PartitionDefinition::getFuncName)
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
        if (rel.isNotBetween()) {
            distributions = new TreeSet<>(RangeUtils.rangeComparator());
            distributions.addAll(ps.calcPartitionRange(null, rel.getStartKey(), true, !rel.isIncludeStart(), ranges));
            distributions.addAll(ps.calcPartitionRange(rel.getEndKey(), null, !rel.isIncludeEnd(), true, ranges));
        } else {
            distributions = ps.calcPartitionRange(
                rel.getStartKey(), rel.getEndKey(), rel.isIncludeStart(), rel.isIncludeEnd(), ranges
            );
        }

        List<Output> outputs = new ArrayList<Output>();

        for (RangeDistribution rd : distributions) {
            PartRangeDeleteOperator operator = new PartRangeDeleteOperator(
                tableInfo.getId(),
                rd.id(),
                td.getDingoType(),
                td.getKeyMapping(),
                rd.getStartKey(),
                rd.getEndKey(),
                rd.isWithStart(),
                rd.isWithEnd()
            );
            Task task = job.getOrCreate(currentLocation, idGenerator);
            operator.setId(idGenerator.getOperatorId(task.getId()));
            task.putOperator(operator);
            OutputHint outputHint = new OutputHint();
            outputHint.setToSumUp(true);
            operator.getSoleOutput().setHint(outputHint);
            outputs.addAll(operator.getOutputs());
        }

        return outputs;
    }
}
