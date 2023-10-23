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

import io.dingodb.calcite.rel.DingoLikeScan;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.LikeScanOperator;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

public final class DingoLikeScanVisitFun {

    private DingoLikeScanVisitFun() {
    }

    public static Collection<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, DingoLikeScan rel
    ) {
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        SqlExpr filter = null;
        if (rel.getFilter() != null) {
            filter = SqlExprUtils.toSqlExpr(rel.getFilter());
        }
        NavigableMap<ComparableByteArray, RangeDistribution> distributions = tableInfo.getRangeDistributions();
        final TableDefinition td = TableUtils.getTableDefinition(rel.getTable());
        final PartitionService ps = PartitionService.getService(
            Optional.ofNullable(td.getPartDefinition())
                .map(PartitionDefinition::getFuncName)
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
        List<Output> outputs = new ArrayList<>();

        for (RangeDistribution distribution : ps.calcPartitionRange(rel.getPrefix(), rel.getPrefix(), true, true, distributions)) {
            LikeScanOperator operator = new LikeScanOperator(
                tableInfo.getId(),
                distribution.id(),
                td.getDingoType(),
                td.getVersion(),
                td.getKeyMapping(),
                Optional.mapOrNull(filter, SqlExpr::copy),
                rel.getSelection(),
                rel.getPrefix()
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(currentLocation, idGenerator);
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }

        return outputs;
    }
}
