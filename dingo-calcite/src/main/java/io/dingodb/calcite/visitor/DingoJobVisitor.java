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

package io.dingodb.calcite.visitor;

import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.rel.DingoCoalesce;
import io.dingodb.calcite.rel.DingoDistributedValues;
import io.dingodb.calcite.rel.DingoExchange;
import io.dingodb.calcite.rel.DingoExchangeRoot;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoHash;
import io.dingodb.calcite.rel.DingoHashJoin;
import io.dingodb.calcite.rel.DingoPartModify;
import io.dingodb.calcite.rel.DingoPartScan;
import io.dingodb.calcite.rel.DingoPartition;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoReduce;
import io.dingodb.calcite.rel.DingoSort;
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.table.TableId;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.expr.RtExprWithType;
import io.dingodb.exec.hash.HashStrategy;
import io.dingodb.exec.hash.SimpleHashStrategy;
import io.dingodb.exec.impl.JobImpl;
import io.dingodb.exec.operator.AggregateOperator;
import io.dingodb.exec.operator.CoalesceOperator;
import io.dingodb.exec.operator.GetByKeysOperator;
import io.dingodb.exec.operator.HashJoinOperator;
import io.dingodb.exec.operator.HashOperator;
import io.dingodb.exec.operator.PartDeleteOperator;
import io.dingodb.exec.operator.PartInsertOperator;
import io.dingodb.exec.operator.PartScanOperator;
import io.dingodb.exec.operator.PartUpdateOperator;
import io.dingodb.exec.operator.PartitionOperator;
import io.dingodb.exec.operator.ProjectOperator;
import io.dingodb.exec.operator.ReceiveOperator;
import io.dingodb.exec.operator.ReduceOperator;
import io.dingodb.exec.operator.RootOperator;
import io.dingodb.exec.operator.SendOperator;
import io.dingodb.exec.operator.SortOperator;
import io.dingodb.exec.operator.SumUpOperator;
import io.dingodb.exec.operator.ValuesOperator;
import io.dingodb.exec.partition.PartitionStrategy;
import io.dingodb.exec.partition.SimplePartitionStrategy;
import io.dingodb.exec.sort.SortCollation;
import io.dingodb.exec.sort.SortDirection;
import io.dingodb.exec.sort.SortNullDirection;
import io.dingodb.meta.Location;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.common.util.Utils.sole;

@Slf4j
public class DingoJobVisitor implements DingoRelVisitor<Collection<Output>> {
    private final IdGenerator idGenerator;
    private final Location currentLocation;
    @Getter
    private final Job job;

    public DingoJobVisitor(IdGenerator idGenerator, Location currentLocation) {
        this.idGenerator = idGenerator;
        this.currentLocation = currentLocation;
        job = new JobImpl(new Id(UUID.randomUUID().toString()));
    }

    @Nonnull
    public static Job createJob(RelNode input, Location currentLocation) {
        return createJob(input, currentLocation, false);
    }

    @Nonnull
    public static Job createJob(RelNode input, Location currentLocation, boolean addRoot) {
        IdGenerator idGenerator = new DingoIdGenerator();
        DingoJobVisitor visitor = new DingoJobVisitor(idGenerator, currentLocation);
        Collection<Output> outputs = dingo(input).accept(visitor);
        if (addRoot) {
            if (outputs.size() == 1) {
                Output output = sole(outputs);
                Task task = output.getTask();
                RootOperator root = new RootOperator(TupleSchema.fromRelDataType(input.getRowType()));
                root.setId(idGenerator.get());
                task.putOperator(root);
                output.setLink(root.getInput(0));
            } else if (!outputs.isEmpty()) {
                throw new IllegalStateException("There must be zero or one output to job root.");
            }
        }
        Job job = visitor.getJob();
        log.info("job = {}", job);
        return job;
    }

    private static Collection<Output> illegalRelNode(@Nonnull RelNode rel) {
        throw new IllegalStateException(
            "RelNode of type \"" + rel.getClass().getSimpleName() + "\" should not appear in a physical plan."
        );
    }

    @Nonnull
    private static SortCollation toSortCollation(@Nonnull RelFieldCollation collation) {
        SortDirection d;
        switch (collation.direction) {
            case DESCENDING:
            case STRICTLY_DESCENDING:
                d = SortDirection.DESCENDING;
                break;
            default:
                d = SortDirection.ASCENDING;
                break;
        }
        SortNullDirection n;
        switch (collation.nullDirection) {
            case FIRST:
                n = SortNullDirection.FIRST;
                break;
            case LAST:
                n = SortNullDirection.LAST;
                break;
            default:
                n = SortNullDirection.UNSPECIFIED;
        }
        return new SortCollation(collation.getFieldIndex(), d, n);
    }

    private Output exchange(@Nonnull Output input, @Nonnull Location target, TupleSchema schema) {
        Task task = input.getTask();
        if (target.equals(task.getLocation())) {
            return input;
        }
        Id id = idGenerator.get();
        Id receiveId = idGenerator.get();
        SendOperator send = new SendOperator(
            target.getHost(),
            target.getPort(),
            receiveId,
            schema
        );
        send.setId(id);
        input.setLink(send.getInput(0));
        task.putOperator(send);
        ReceiveOperator receive = new ReceiveOperator(
            task.getHost(),
            task.getLocation().getPort(),
            schema
        );
        receive.setId(receiveId);
        receive.getSoleOutput().copyHint(input);
        Task rcvTask = job.getOrCreate(target, idGenerator);
        rcvTask.putOperator(receive);
        return receive.getSoleOutput();
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoAggregate rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        List<Output> outputs = new LinkedList<>();
        for (Output input : inputs) {
            Operator operator = new AggregateOperator(rel.getKeys(), rel.getAggList());
            Task task = input.getTask();
            operator.setId(idGenerator.get());
            task.putOperator(operator);
            input.setLink(operator.getInput(0));
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoCoalesce rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        // Coalesce inputs from the same task. taskId --> list of inputs
        Map<Id, List<Output>> inputsMap = new HashMap<>();
        for (Output input : inputs) {
            Id taskId = input.getTaskId();
            List<Output> list = inputsMap.computeIfAbsent(taskId, k -> new LinkedList<>());
            list.add(input);
        }
        List<Output> outputs = new LinkedList<>();
        for (Map.Entry<Id, List<Output>> entry : inputsMap.entrySet()) {
            List<Output> list = entry.getValue();
            int size = list.size();
            if (size <= 1) {
                // Need no coalescing.
                outputs.addAll(list);
            } else {
                Output one = list.get(0);
                Task task = one.getTask();
                Operator operator = new CoalesceOperator(size);
                operator.setId(idGenerator.get());
                task.putOperator(operator);
                int i = 0;
                for (Output input : list) {
                    input.setLink(operator.getInput(i));
                    ++i;
                }
                if (one.isToSumUp()) {
                    Operator sumUpOperator = new SumUpOperator();
                    sumUpOperator.setId(idGenerator.get());
                    task.putOperator(sumUpOperator);
                    operator.getSoleOutput().setLink(sumUpOperator.getInput(0));
                    outputs.addAll(sumUpOperator.getOutputs());
                } else {
                    outputs.addAll(operator.getOutputs());
                }
            }
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoDistributedValues rel) {
        List<Output> outputs = new LinkedList<>();
        MetaHelper metaHelper = new MetaHelper(rel.getTable());
        final Map<String, Location> partLocations = metaHelper.getPartLocations();
        final TableDefinition td = metaHelper.getTableDefinition();
        final PartitionStrategy ps = new SimplePartitionStrategy(partLocations.size());
        Map<String, List<Object[]>> partMap = ps.partTuples(
            rel.getValues(),
            td.getKeyMapping()
        );
        for (Map.Entry<String, List<Object[]>> entry : partMap.entrySet()) {
            Object partId = entry.getKey();
            ValuesOperator operator = new ValuesOperator(
                entry.getValue(),
                TupleSchema.fromRelDataType(rel.getRowType())
            );
            operator.setId(idGenerator.get());
            OutputHint hint = new OutputHint();
            hint.setPartId(partId);
            Location location = partLocations.get(partId);
            hint.setLocation(location);
            operator.getSoleOutput().setHint(hint);
            Task task = job.getOrCreate(location, idGenerator);
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoExchange rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        List<Output> outputs = new LinkedList<>();
        TupleSchema schema = TupleSchema.fromRelDataType(rel.getRowType());
        for (Output input : inputs) {
            outputs.add(exchange(input, input.getTargetLocation(), schema));
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoExchangeRoot rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        List<Output> outputs = new LinkedList<>();
        TupleSchema schema = TupleSchema.fromRelDataType(rel.getRowType());
        for (Output input : inputs) {
            outputs.add(exchange(input, currentLocation, schema));
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoGetByKeys rel) {
        MetaHelper metaHelper = new MetaHelper(rel.getTable());
        final Map<String, Location> partLocations = metaHelper.getPartLocations();
        final TableDefinition td = metaHelper.getTableDefinition();
        final TableId tableId = metaHelper.getTableId();
        final PartitionStrategy ps = new SimplePartitionStrategy(partLocations.size());
        Map<String, List<Object[]>> partMap = ps.partKeyTuples(rel.getKeyTuples());
        List<Output> outputs = new LinkedList<>();
        for (Map.Entry<String, List<Object[]>> entry : partMap.entrySet()) {
            final Object partId = entry.getKey();
            GetByKeysOperator operator = new GetByKeysOperator(
                tableId,
                partId,
                td.getTupleSchema(),
                td.getKeyMapping(),
                entry.getValue(),
                rel.getSelection()
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(partLocations.get(entry.getKey()), idGenerator);
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoHash rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        List<Output> outputs = new LinkedList<>();
        final Collection<Location> locations = Services.CLUSTER.getComputingLocations();
        final HashStrategy hs = new SimpleHashStrategy();
        for (Output input : inputs) {
            Task task = input.getTask();
            HashOperator operator = new HashOperator(hs, rel.getMapping());
            operator.setId(idGenerator.get());
            operator.createOutputs(locations);
            task.putOperator(operator);
            input.setLink(operator.getInput(0));
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoHashJoin rel) {
        Collection<Output> leftInputs = dingo(rel.getLeft()).accept(this);
        Collection<Output> rightInputs = dingo(rel.getRight()).accept(this);
        Map<Id, Output> leftInputsMap = new HashMap<>(leftInputs.size());
        Map<Id, Output> rightInputsMap = new HashMap<>(rightInputs.size());
        // Only one left input in each task, because of coalescing.
        leftInputs.forEach(i -> leftInputsMap.put(i.getTaskId(), i));
        rightInputs.forEach(i -> rightInputsMap.put(i.getTaskId(), i));
        List<Output> outputs = new LinkedList<>();
        for (Map.Entry<Id, Output> entry : leftInputsMap.entrySet()) {
            Id taskId = entry.getKey();
            Output left = entry.getValue();
            Output right = rightInputsMap.get(taskId);
            Operator operator = new HashJoinOperator(rel.getLeftMapping(), rel.getRightMapping());
            operator.setId(idGenerator.get());
            left.setLink(operator.getInput(0));
            right.setLink(operator.getInput(1));
            Task task = job.getTask(taskId);
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoPartition rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        List<Output> outputs = new LinkedList<>();
        MetaHelper metaHelper = new MetaHelper(rel.getTable());
        final Map<String, Location> partLocations = metaHelper.getPartLocations();
        final TableDefinition td = metaHelper.getTableDefinition();
        final PartitionStrategy ps = new SimplePartitionStrategy(partLocations.size());
        for (Output input : inputs) {
            Task task = input.getTask();
            PartitionOperator operator = new PartitionOperator(
                ps,
                td.getKeyMapping()
            );
            operator.setId(idGenerator.get());
            operator.createOutputs(partLocations);
            task.putOperator(operator);
            input.setLink(operator.getInput(0));
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoPartModify rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        List<Output> outputs = new LinkedList<>();
        MetaHelper metaHelper = new MetaHelper(rel.getTable());
        TableDefinition td = metaHelper.getTableDefinition();
        final TableId tableId = metaHelper.getTableId();
        for (Output input : inputs) {
            Task task = input.getTask();
            Operator operator;
            switch (rel.getOperation()) {
                case INSERT:
                    operator = new PartInsertOperator(
                        tableId,
                        input.getHint().getPartId(),
                        td.getTupleSchema(),
                        td.getKeyMapping()
                    );
                    break;
                case UPDATE:
                    operator = new PartUpdateOperator(
                        tableId,
                        input.getHint().getPartId(),
                        td.getTupleSchema(),
                        td.getKeyMapping(),
                        TupleMapping.of(td.getColumnIndices(rel.getUpdateColumnList())),
                        rel.getSourceExpressionList().stream()
                            .map(RexConverter::toRtExprWithType)
                            .collect(Collectors.toList())
                    );
                    break;
                case DELETE:
                    operator = new PartDeleteOperator(
                        tableId,
                        input.getHint().getPartId(),
                        td.getTupleSchema(),
                        td.getKeyMapping()
                    );
                    break;
                default:
                    throw new IllegalStateException(
                        "Operation \"" + rel.getOperation() + "\" is not supported."
                    );
            }
            operator.setId(idGenerator.get());
            task.putOperator(operator);
            input.setLink(operator.getInput(0));
            OutputHint hint = new OutputHint();
            hint.setToSumUp(true);
            operator.getSoleOutput().setHint(hint);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoPartScan rel) {
        MetaHelper metaHelper = new MetaHelper(rel.getTable());
        TableDefinition td = metaHelper.getTableDefinition();
        Map<String, Location> parts = metaHelper.getPartLocations();
        TableId tableId = metaHelper.getTableId();
        List<Output> outputs = new ArrayList<>(parts.size());
        RtExprWithType filter = null;
        if (rel.getFilter() != null) {
            filter = RexConverter.toRtExprWithType(rel.getFilter());
        }
        for (Map.Entry<String, Location> entry : parts.entrySet()) {
            PartScanOperator operator = new PartScanOperator(
                tableId,
                entry.getKey(),
                td.getTupleSchema(),
                td.getKeyMapping(),
                filter,
                rel.getSelection()
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(entry.getValue(), idGenerator);
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoProject rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        List<Output> outputs = new ArrayList<>(inputs.size());
        for (Output input : inputs) {
            Operator operator = new ProjectOperator(
                RexConverter.toRtExprWithType(rel.getProjects()),
                TupleSchema.fromRelDataType(rel.getInput().getRowType())
            );
            Task task = input.getTask();
            operator.setId(idGenerator.get());
            task.putOperator(operator);
            input.setLink(operator.getInput(0));
            operator.getSoleOutput().copyHint(input);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoReduce rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        Operator operator;
        operator = new ReduceOperator(rel.getKeyMapping(), rel.getAggList());
        operator.setId(idGenerator.get());
        Output input = sole(inputs);
        Task task = input.getTask();
        task.putOperator(operator);
        input.setLink(operator.getInput(0));
        return operator.getOutputs();
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoSort rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        List<Output> outputs = new LinkedList<>();
        for (Output input : inputs) {
            Operator operator = new SortOperator(
                rel.getCollation().getFieldCollations().stream()
                    .map(DingoJobVisitor::toSortCollation)
                    .collect(Collectors.toList()),
                rel.fetch == null ? -1 : RexLiteral.intValue(rel.fetch),
                rel.offset == null ? 0 : RexLiteral.intValue(rel.offset)
            );
            Task task = input.getTask();
            operator.setId(idGenerator.get());
            task.putOperator(operator);
            input.setLink(operator.getInput(0));
            operator.getSoleOutput().copyHint(input);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoTableModify rel) {
        return illegalRelNode(rel);
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoTableScan rel) {
        return illegalRelNode(rel);
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoValues rel) {
        Task task = job.getOrCreate(currentLocation, idGenerator);
        ValuesOperator operator = new ValuesOperator(
            rel.getValues(),
            TupleSchema.fromRelDataType(rel.getRowType())
        );
        operator.setId(idGenerator.get());
        task.putOperator(operator);
        return operator.getOutputs();
    }
}
