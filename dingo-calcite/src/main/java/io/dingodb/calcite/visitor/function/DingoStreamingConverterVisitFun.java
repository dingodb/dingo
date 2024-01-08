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

import io.dingodb.calcite.rel.DingoStreamingConverter;
import io.dingodb.calcite.traits.DingoRelPartition;
import io.dingodb.calcite.traits.DingoRelPartitionByKeys;
import io.dingodb.calcite.traits.DingoRelPartitionByTable;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.cluster.ClusterService;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.hash.HashStrategy;
import io.dingodb.exec.operator.hash.SimpleHashStrategy;
import io.dingodb.exec.operator.params.HashParam;
import io.dingodb.exec.operator.params.PartitionParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.meta.MetaService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.exec.utils.OperatorCodeUtils.HASH;
import static io.dingodb.exec.utils.OperatorCodeUtils.PARTITION;

@Slf4j
public class DingoStreamingConverterVisitFun {
    @NonNull
    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, ITransaction transaction, DingoJobVisitor visitor, DingoStreamingConverter rel
    ) {
        return convertStreaming(
            job, idGenerator, currentLocation,
            transaction,
            dingo(rel.getInput()).accept(visitor),
            dingo(rel.getInput()).getStreaming(),
            rel.getStreaming(),
            DefinitionMapper.mapToDingoType(rel.getRowType())
        );
    }

    public static @NonNull Collection<Vertex> convertStreaming(
        Job job, IdGenerator idGenerator, Location currentLocation,
        ITransaction transaction,
        @NonNull Collection<Vertex> inputs,
        @NonNull DingoRelStreaming srcStreaming,
        @NonNull DingoRelStreaming dstStreaming,
        DingoType schema
    ) {
        final Set<DingoRelPartition> dstPartitions = dstStreaming.getPartitions();
        final Set<DingoRelPartition> srcPartitions = srcStreaming.getPartitions();
        assert dstPartitions != null && srcPartitions != null;
        final DingoRelPartition dstDistribution = dstStreaming.getDistribution();
        final DingoRelPartition srcDistribution = srcStreaming.getDistribution();
        DingoRelStreaming media = dstStreaming.withPartitions(srcPartitions);
        assert media.getPartitions() != null;
        Collection<Vertex> outputs = inputs;
        if (media.getPartitions().size() > srcPartitions.size()) {
            for (DingoRelPartition partition : media.getPartitions()) {
                if (!srcPartitions.contains(partition)) {
                    if (partition instanceof DingoRelPartitionByTable) {
                        outputs = partition(idGenerator, outputs, (DingoRelPartitionByTable) partition);
                    } else if (partition instanceof DingoRelPartitionByKeys) {
                        outputs = hash(idGenerator, outputs, (DingoRelPartitionByKeys) partition);
                    } else {
                        throw new IllegalStateException("Not supported.");
                    }
                }
            }
        }
        if (!Objects.equals(dstDistribution, srcDistribution)) {
            outputs = outputs.stream().map(input -> {
                Location targetLocation = (dstDistribution == null ? currentLocation : input.getTargetLocation());
                return DingoExchangeFun.exchange(job, idGenerator, transaction, input, targetLocation, schema);
            }).collect(Collectors.toList());
        }
        if (dstPartitions.size() < media.getPartitions().size()) {
            assert dstDistribution == null && dstPartitions.size() == 0 || dstPartitions.size() == 1;
            outputs = DingoCoalesce.coalesce(idGenerator, outputs, dstPartitions, media.getPartitions());
        }
        return outputs;
    }

    private static @NonNull Collection<Vertex> partition(
        IdGenerator idGenerator,
        @NonNull Collection<Vertex> inputs,
        @NonNull DingoRelPartitionByTable partition
    ) {
        List<Vertex> outputs = new LinkedList<>();
        final TableInfo tableInfo = MetaServiceUtils.getTableInfo(partition.getTable());
        final TableDefinition td = TableUtils.getTableDefinition(partition.getTable());
        NavigableMap<ComparableByteArray, RangeDistribution> distributions = tableInfo.getRangeDistributions();
        for (Vertex input : inputs) {
            Task task = input.getTask();
            PartitionParam param = new PartitionParam(distributions, td);
            Vertex vertex = new Vertex(PARTITION, param);
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            OutputHint hint = new OutputHint();
            hint.setLocation(MetaService.root().currentLocation());
            vertex.setHint(hint);
            Edge edge = new Edge(input, vertex);
            vertex.addIn(edge);
            task.putVertex(vertex);
            input.addEdge(edge);
            outputs.add(vertex);
        }
        return outputs;
    }

    private static @NonNull Collection<Vertex> hash(
        IdGenerator idGenerator,
        @NonNull Collection<Vertex> inputs,
        @NonNull DingoRelPartitionByKeys hash
    ) {
        List<Vertex> outputs = new LinkedList<>();
        final List<Location> locations = new ArrayList<>(ClusterService.getDefault().getComputingLocations());
        final HashStrategy hs = new SimpleHashStrategy();
        for (Vertex input : inputs) {
            Task task = input.getTask();
            HashParam param = new HashParam(hs, TupleMapping.of(hash.getKeys()));
            Vertex vertex = new Vertex(HASH, param);
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            OutputHint hint = new OutputHint();
            hint.setLocation(locations.get(0));
            vertex.setHint(hint);
            Edge edge = new Edge(input, vertex);
            vertex.addIn(edge);
            task.putVertex(vertex);
            input.addEdge(edge);
            outputs.add(vertex);
        }
        return outputs;
    }

}
