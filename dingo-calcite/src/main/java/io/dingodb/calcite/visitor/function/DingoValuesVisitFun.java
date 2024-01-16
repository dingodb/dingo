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

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.calcite.traits.DingoRelPartition;
import io.dingodb.calcite.traits.DingoRelPartitionByTable;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.ValuesParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.utils.OperatorCodeUtils.VALUES;

public final class DingoValuesVisitFun {
    private DingoValuesVisitFun() {
    }

    public static List<Vertex> visit(
        Job job,
        IdGenerator idGenerator,
        Location currentLocation,
        ITransaction transaction,
        DingoJobVisitor visitor,
        @NonNull DingoValues rel
    ) {
        DingoRelStreaming streaming = rel.getStreaming();
        if (streaming.equals(DingoRelStreaming.ROOT)) {
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
            ValuesParam param = new ValuesParam(rel.getTuples(),
                Objects.requireNonNull(DefinitionMapper.mapToDingoType(rel.getRowType()))
            );
            Vertex vertex = new Vertex(VALUES, param);
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            return ImmutableList.of(vertex);
        }
        DingoRelPartition distribution = streaming.getDistribution();
        if (distribution instanceof DingoRelPartitionByTable) {
            List<Vertex> outputs = new LinkedList<>();
            final TableInfo tableInfo = MetaServiceUtils.getTableInfo(
                ((DingoRelPartitionByTable) distribution).getTable());
            final Table td = ((DingoRelPartitionByTable) distribution).getTable().unwrap(DingoTable.class).getTable();
            final KeyValueCodec keyValueCodec = TableUtils.getKeyValueCodecForTable(td);
            final NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions
                = tableInfo.getRangeDistributions();
            final PartitionService ps = PartitionService.getService(
                Optional.ofNullable(td.getPartitionStrategy()).orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
            Map<CommonId, List<Object[]>> partMap =
                ps.partTuples(rel.getTuples(), wrap(keyValueCodec::encodeKey), distributions);
            for (Map.Entry<CommonId, List<Object[]>> entry : partMap.entrySet()) {
                ValuesParam param = new ValuesParam(
                    entry.getValue(),
                    Objects.requireNonNull(DefinitionMapper.mapToDingoType(rel.getRowType()))
                );
                Vertex vertex = new Vertex(VALUES, param);
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
                vertex.setId(idGenerator.getOperatorId(task.getId()));
                OutputHint hint = new OutputHint();
                hint.setPartId(entry.getKey());
                hint.setLocation(currentLocation);
                vertex.setHint(hint);
                task.putVertex(vertex);
                outputs.add(vertex);
            }
            return outputs;
        }
        throw new IllegalArgumentException("Unsupported streaming \"" + streaming + "\" of values.");
    }
}
