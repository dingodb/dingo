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

import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoFunctionScan;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.DistributionSourceParam;
import io.dingodb.exec.operator.params.PartRangeScanParam;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.exec.utils.OperatorCodeUtils.CALC_DISTRIBUTION;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_RANGE_SCAN;

@Slf4j
public final class DingoFunctionScanVisitFun {

    private DingoFunctionScanVisitFun() {
    }

    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, DingoFunctionScan rel
    ) {
        DingoRelOptTable relTable = rel.getTable();
        DingoTable dingoTable = relTable.unwrap(DingoTable.class);

        MetaService metaService = MetaService.root().getSubMetaService(relTable.getSchemaName());
        CommonId tableId = dingoTable.getTableId();
        Table td = dingoTable.getTable();
        NavigableMap<ComparableByteArray, RangeDistribution> ranges = metaService.getRangeDistribution(tableId);

        DistributionSourceParam distributionParam = new DistributionSourceParam(
            td,
            ranges,
            null,
            null,
            true,
            true,
            null,
            false,
            false,
            null);
        Vertex calcVertex = new Vertex(CALC_DISTRIBUTION, distributionParam);
        Task task = job.getOrCreate(currentLocation, idGenerator);
        calcVertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(calcVertex);

        List<Vertex> outputs = new ArrayList<>();

        for (int i = 0; i <= Optional.mapOrGet(td.getPartitions(), List::size, () -> 0); i++) {
            PartRangeScanParam param = new PartRangeScanParam(
                tableId,
                td.tupleType(),
                td.keyMapping(),
                null,
                td.mapping(),
                null,
                null,
                td.tupleType(),
                false
            );
            task = job.getOrCreate(currentLocation, idGenerator);
            Vertex scanVertex = new Vertex(PART_RANGE_SCAN, param);
            scanVertex.setId(idGenerator.getOperatorId(task.getId()));
            Edge edge = new Edge(calcVertex, scanVertex);
            calcVertex.addEdge(edge);
            scanVertex.addIn(edge);
            task.putVertex(scanVertex);
            outputs.add(scanVertex);
        }
        return outputs;
    }
}
