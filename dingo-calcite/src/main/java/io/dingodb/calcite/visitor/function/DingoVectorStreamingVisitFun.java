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
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.CoalesceOperator;
import io.dingodb.exec.operator.VectorPartitionOperator;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
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

public final class DingoVectorStreamingVisitFun {

    public static Collection<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, VectorStreamConvertor rel
    ) {
        List<Output> outputs = new LinkedList<>();
        Collection<Output> inputs = dingo(rel.getInput()).accept(visitor);
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
        TableDefinition td = TableUtils.getTableDefinition(partition.getTable());

        String schemaName = dingoRelOptTable.getSchemaName();
        MetaService metaService = MetaService.root().getSubMetaService(schemaName);
        CommonId indexId = rel.getIndexId();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions
            = metaService.getIndexRangeDistribution(rel.getIndexId());

        for (Output input : inputs) {
            Task task = input.getTask();
            VectorPartitionOperator operator = new VectorPartitionOperator(
                tableInfo.getId(),
                distributions,
                indexId,
                rel.getVectorIdIndex(),
                rel.getIndexTableDefinition());
            operator.setId(idGenerator.getOperatorId(task.getId()));
            operator.createOutputs(distributions);
            task.putOperator(operator);
            input.setLink(operator.getInput(0));
            outputs.addAll(operator.getOutputs());
        }
        // coalesce
        outputs = coalesce(idGenerator, outputs);
        return outputs;
    }

    public static List<Output> coalesce(IdGenerator idGenerator, List<Output> inputList) {
        Map<CommonId, List<Output>> inputsMap = new LinkedHashMap<>();
        for (Output input : inputList) {
            List<Output> list = inputsMap.computeIfAbsent(input.getHint().getPartId(), k -> new LinkedList<>());
            list.add(input);
        }
        List<Output> outputs = new LinkedList<>();
        for (Map.Entry<CommonId, List<Output>> entry : inputsMap.entrySet()) {
            List<Output> list = entry.getValue();
            int size = list.size();
            if (list.size() <= 1) {
                // Need no coalescing.
                outputs.addAll(list);
            } else {
                Output one = list.get(0);
                Task task = one.getTask();
                Operator operator = new CoalesceOperator(size);
                operator.setId(idGenerator.getOperatorId(task.getId()));
                task.putOperator(operator);
                int i = 0;
                for (Output input : list) {
                    input.setLink(operator.getInput(i));
                    ++i;
                }
                Output newOutput = operator.getSoleOutput();
                newOutput.copyHint(one);
                outputs.add(newOutput);
            }
        }
        return outputs;
    }

}
