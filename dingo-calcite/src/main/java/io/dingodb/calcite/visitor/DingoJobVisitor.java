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
import io.dingodb.calcite.rel.DingoGetByKeys;
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
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.impl.JobImpl;
import io.dingodb.exec.operator.AggregateOperator;
import io.dingodb.exec.operator.CoalesceOperator;
import io.dingodb.exec.operator.GetByKeysOperator;
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
import io.dingodb.exec.operator.SortCollation;
import io.dingodb.exec.operator.SortOperator;
import io.dingodb.exec.operator.SumUpOperator;
import io.dingodb.exec.operator.ValuesOperator;
import io.dingodb.exec.partition.PartitionStrategy;
import io.dingodb.exec.partition.SimpleHashStrategy;
import io.dingodb.meta.Location;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collection;
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
        job = new JobImpl(UUID.randomUUID().toString());
    }

    @Nonnull
    public static Job createJob(RelNode input, Location currentLocation) {
        return createJob(input, currentLocation, false);
    }

    @Nonnull
    public static Job createJob(RelNode input, Location currentLocation, boolean addRoot) {
        IdGenerator idGenerator = new IdGenerator();
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
    private static String getSimpleName(@Nonnull RelOptTable table) {
        // skip the root schema name.
        return String.join(".", Util.skip(table.getQualifiedName()));
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
        int size = inputs.size();
        if (size <= 1) {
            return inputs;
        }
        Output one = inputs.iterator().next();
        boolean isToSumUp = one.isToSumUp();
        Operator operator = isToSumUp ? new SumUpOperator(size) : new CoalesceOperator(size);
        operator.setId(idGenerator.get());
        Task task = one.getTask();
        task.putOperator(operator);
        int i = 0;
        for (Output input : inputs) {
            assert input.getTask().equals(task) : "Operator linked must be in the same task.";
            assert input.isToSumUp() == isToSumUp : "All inputs must have the same \"toSumUp\" hint.";
            input.setLink(operator.getInput(i));
            ++i;
        }
        return operator.getOutputs();
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoDistributedValues rel) {
        List<Output> outputs = new LinkedList<>();
        MetaHelper metaHelper = new MetaHelper(rel.getTable());
        final Map<String, Location> partLocations = metaHelper.getPartLocations();
        final TableDefinition td = metaHelper.getTableDefinition();
        final PartitionStrategy ps = new SimpleHashStrategy(partLocations.size());
        Map<String, List<Object[]>> partMap = ps.partTuples(
            rel.getValues(),
            td.getKeyMapping()
        );
        for (Map.Entry<String, List<Object[]>> entry : partMap.entrySet()) {
            Object partId = entry.getKey();
            ValuesOperator operator = new ValuesOperator(entry.getValue());
            operator.setId(idGenerator.get());
            OutputHint hint = new OutputHint();
            hint.setPartId(partId);
            Location location = partLocations.get(partId);
            hint.setLocation(location);
            operator.getSoleOutput().setHint(hint);
            Task task = job.getOrCreate(location);
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
            Task task = input.getTask();
            Location target = input.getTargetLocation();
            if (target == null) {
                target = currentLocation;
            }
            if (!target.equals(task.getLocation())) {
                Id id = idGenerator.get();
                Id receiveId = idGenerator.get();
                SendOperator send = new SendOperator(
                    target.getHost(),
                    target.getPort(),
                    receiveId,
                    schema
                );
                send.setId(id);
                task.putOperator(send);
                input.setLink(send.getInput(0));
                ReceiveOperator receive = new ReceiveOperator(
                    task.getHost(),
                    task.getLocation().getPort(),
                    schema
                );
                receive.setId(receiveId);
                receive.getSoleOutput().copyHint(input);
                Task rcvTask = job.getOrCreate(target);
                rcvTask.putOperator(receive);
                outputs.addAll(receive.getOutputs());
            } else {
                outputs.add(input);
            }
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoGetByKeys rel) {
        MetaHelper metaHelper = new MetaHelper(rel.getTable());
        final Map<String, Location> partLocations = metaHelper.getPartLocations();
        final TableDefinition td = metaHelper.getTableDefinition();
        final TableId tableId = metaHelper.getTableId();
        final PartitionStrategy ps = new SimpleHashStrategy(partLocations.size());
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
            Task task = job.getOrCreate(partLocations.get(entry.getKey()));
            task.putOperator(operator);
            operator.getSoleOutput().setHint(OutputHint.of(metaHelper.getTableName(), partId));
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
        final PartitionStrategy ps = new SimpleHashStrategy(partLocations.size());
        for (Output input : inputs) {
            Task task = input.getTask();
            PartitionOperator operator = new PartitionOperator(
                ps,
                td.getKeyMapping()
            );
            operator.setId(idGenerator.get());
            operator.createOutputs(metaHelper.getTableName(), partLocations);
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
                            .map(RexConverter::toString)
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
        String filterStr = null;
        if (rel.getFilter() != null) {
            filterStr = RexConverter.convert(rel.getFilter()).toString();
        }
        for (Map.Entry<String, Location> entry : parts.entrySet()) {
            final Object partId = entry.getKey();
            PartScanOperator operator = new PartScanOperator(
                tableId,
                entry.getKey(),
                td.getTupleSchema(),
                td.getKeyMapping(),
                filterStr,
                rel.getSelection()
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(entry.getValue());
            task.putOperator(operator);
            operator.getSoleOutput().setHint(OutputHint.of(metaHelper.getTableName(), partId));
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
                RexConverter.toString(rel.getProjects()),
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
        int size = inputs.size();
        if (size <= 1) {
            return inputs;
        }
        Operator operator;
        operator = new ReduceOperator(inputs.size(), rel.getKeyMapping(), rel.getAggList());
        operator.setId(idGenerator.get());
        Output one = inputs.iterator().next();
        Task task = one.getTask();
        task.putOperator(operator);
        int i = 0;
        for (Output input : inputs) {
            assert input.getTask().equals(task) : "Operator linked must be in the same task.";
            input.setLink(operator.getInput(i));
            ++i;
        }
        return operator.getOutputs();
    }

    @Override
    public Collection<Output> visit(@Nonnull DingoSort rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        List<Output> outputs = new LinkedList<>();
        for (Output input : inputs) {
            Operator operator = new SortOperator(
                rel.getCollation().getFieldCollations().stream()
                    .map(c -> new SortCollation(c.getFieldIndex(), c.direction, c.nullDirection))
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
        Task task = job.getOrCreate(currentLocation);
        ValuesOperator operator = new ValuesOperator(rel.getValues());
        operator.setId(idGenerator.get());
        task.putOperator(operator);
        return operator.getOutputs();
    }
}
