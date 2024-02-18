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
import io.dingodb.calcite.rel.DingoLikeScan;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.params.DistributionSourceParam;
import io.dingodb.exec.operator.params.LikeScanParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.transaction.data.IsolationLevel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.exec.utils.OperatorCodeUtils.CALC_DISTRIBUTION;
import static io.dingodb.exec.utils.OperatorCodeUtils.LIKE_SCAN;

public final class DingoLikeScanVisitFun {

    private DingoLikeScanVisitFun() {
    }

    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation,
        ITransaction transaction, DingoJobVisitor visitor, DingoLikeScan rel
    ) {
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        SqlExpr filter = null;
        if (rel.getFilter() != null) {
            filter = SqlExprUtils.toSqlExpr(rel.getFilter());
        }
        NavigableMap<ComparableByteArray, RangeDistribution> distributions = tableInfo.getRangeDistributions();
        final Table td = rel.getTable().unwrap(DingoTable.class).getTable();
        final PartitionService ps = PartitionService.getService(
            Optional.ofNullable(td.getPartitionStrategy())
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
        List<Vertex> outputs = new ArrayList<>();

        DistributionSourceParam distributionSourceParam = new DistributionSourceParam(
            td,
            distributions,
            rel.getPrefix(),
            rel.getPrefix(),
            true,
            true,
            null,
            false,
            false,
            null);
        Vertex calcVertex = new Vertex(CALC_DISTRIBUTION, distributionSourceParam);

        for (RangeDistribution distribution
            : ps.calcPartitionRange(rel.getPrefix(), rel.getPrefix(), true, true, distributions)) {
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
            LikeScanParam param = new LikeScanParam(
                tableInfo.getId(),
                distribution.id(),
                td.tupleType(),
                td.keyMapping(),
                Optional.mapOrNull(filter, SqlExpr::copy),
                rel.getSelection(),
                rel.getPrefix()
            );
            Vertex vertex = new Vertex(LIKE_SCAN, param);
            OutputHint hint = new OutputHint();
            hint.setPartId(distribution.id());
            vertex.setHint(hint);
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            outputs.add(vertex);
        }

        return outputs;
    }
}
