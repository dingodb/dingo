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
import io.dingodb.calcite.rel.VectorStreamConvertor;
import io.dingodb.calcite.traits.DingoRelPartition;
import io.dingodb.calcite.traits.DingoRelPartitionByTable;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.CoalesceParam;
import io.dingodb.exec.operator.params.VectorPartitionParam;
import io.dingodb.meta.MetaService;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.exec.utils.OperatorCodeUtils.COALESCE;
import static io.dingodb.exec.utils.OperatorCodeUtils.VECTOR_PARTITION;

public final class DingoVectorStreamingVisitFun {

    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, VectorStreamConvertor rel
    ) {
        List<Vertex> outputs = new LinkedList<>();
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);
        if (!rel.isNeedRoute()) {
            outputs = DingoCoalesce.coalesce(idGenerator, inputs);
            return outputs;
        }

        Set<DingoRelPartition> partitionSet = dingo(rel.getInput()).getStreaming().getPartitions();
        // for loop inputs (part scan)
        // vector partitionOperator => loop vector regions =>  Collection<Output>
        Optional<DingoRelPartition> found = Optional.empty();
        assert partitionSet != null;
        for (DingoRelPartition dingoRelPartition : partitionSet) {
            found = Optional.of(dingoRelPartition);
            break;
        }
        DingoRelPartitionByTable partition = (DingoRelPartitionByTable) found.get();
        DingoRelOptTable dingoRelOptTable = (DingoRelOptTable)partition.getTable();
        final TableInfo tableInfo = MetaServiceUtils.getTableInfo(dingoRelOptTable);

        String schemaName = dingoRelOptTable.getSchemaName();
        MetaService metaService = MetaService.root().getSubMetaService(schemaName);
        CommonId indexId = rel.getIndexId();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions
            = metaService.getRangeDistribution(rel.getIndexId());

        for (Vertex input : inputs) {
            Task task = input.getTask();
            VectorPartitionParam param = new VectorPartitionParam(
                tableInfo.getId(),
                distributions,
                indexId,
                rel.getVectorIdIndex(),
                rel.getIndexTableDefinition());
            Vertex vertex = new Vertex(VECTOR_PARTITION, param);
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            OutputHint hint = new OutputHint();
            hint.setLocation(MetaService.root().currentLocation());
            vertex.setHint(hint);
            input.setPin(0);
            Edge edge = new Edge(input, vertex);
            input.addEdge(edge);
            vertex.addIn(edge);
            task.putVertex(vertex);
            input.setPin(0);
            outputs.add(vertex);
        }
        // coalesce
        outputs = coalesce(idGenerator, outputs);
        return outputs;
    }

    public static List<Vertex> coalesce(IdGenerator idGenerator, List<Vertex> inputList) {
        Map<CommonId, List<Vertex>> inputsMap = new LinkedHashMap<>();
        for (Vertex input : inputList) {
            List<Vertex> list = inputsMap.computeIfAbsent(input.getHint().getPartId(), k -> new LinkedList<>());
            list.add(input);
        }
        List<Vertex> outputs = new LinkedList<>();
        for (Map.Entry<CommonId, List<Vertex>> entry : inputsMap.entrySet()) {
            List<Vertex> list = entry.getValue();
            int size = list.size();
            if (list.size() <= 1) {
                // Need no coalescing.
                outputs.addAll(list);
            } else {
                Vertex one = list.get(0);
                Task task = one.getTask();
                Vertex vertex = new Vertex(COALESCE, new CoalesceParam(size));
                vertex.setId(idGenerator.getOperatorId(task.getId()));
                task.putVertex(vertex);
                int i = 0;
                for (Vertex input : list) {
                    input.addEdge(new Edge(input, vertex));
                    input.setPin(i);
                    ++i;
                }
                vertex.addIn(new Edge(one, vertex));
                vertex.copyHint(one);
                outputs.add(vertex);
            }
        }
        return outputs;
    }

}
