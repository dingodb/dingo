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
import io.dingodb.calcite.rel.DingoPartCountDelete;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.Distribution;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.PartCountParam;
import io.dingodb.exec.operator.params.RemovePartParam;
import io.dingodb.meta.entity.Table;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.exec.utils.OperatorCodeUtils.PART_COUNT;
import static io.dingodb.exec.utils.OperatorCodeUtils.REMOVE_PART;

public final class DingoCountDeleteVisitFun {
    private DingoCountDeleteVisitFun() {
    }

    @NonNull
    public static List<Vertex> visit(
        Job job,
        IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        @NonNull DingoPartCountDelete rel
    ) {
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        final Table td = rel.getTable().unwrap(DingoTable.class).getTable();
        CommonId tableId = tableInfo.getId();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions
            = tableInfo.getRangeDistributions();

        List<Vertex> outputs = new ArrayList<>(distributions.size());
        for (Distribution distribution : distributions.values()) {
            Task task = job.getOrCreate(currentLocation, idGenerator);
            Vertex vertex;
            if (rel.isDoDeleting()) {
                RemovePartParam param = new RemovePartParam(
                    tableId, distribution.id(), td.tupleType(), td.keyMapping());
                vertex = new Vertex(REMOVE_PART, param);
            } else {
                PartCountParam param = new PartCountParam(
                    tableId, distribution.id(), td.tupleType(), td.keyMapping());
                vertex = new Vertex(PART_COUNT, param);
            }
            vertex.setId(idGenerator.getOperatorId(task.getId().seq));
            task.putVertex(vertex);
            OutputHint hint = new OutputHint();
            hint.setToSumUp(true);
            vertex.setHint(hint);
            outputs.add(vertex);
        }
        return outputs;
    }
}
