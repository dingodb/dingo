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

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.MetaCache;
import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.rel.DingoFilter;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoHashJoin;
import io.dingodb.calcite.rel.DingoLikeScan;
import io.dingodb.calcite.rel.DingoPartCountDelete;
import io.dingodb.calcite.rel.DingoPartRangeDelete;
import io.dingodb.calcite.rel.DingoPartRangeScan;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoReduce;
import io.dingodb.calcite.rel.DingoRoot;
import io.dingodb.calcite.rel.DingoSort;
import io.dingodb.calcite.rel.DingoStreamingConverter;
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.DingoUnion;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.calcite.traits.DingoRelPartition;
import io.dingodb.calcite.traits.DingoRelPartitionByKeys;
import io.dingodb.calcite.traits.DingoRelPartitionByTable;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.RexLiteralUtils;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.PartitionStrategy;
import io.dingodb.common.partition.RangeStrategy;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.exec.Services;
import io.dingodb.exec.aggregate.Agg;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.AggregateOperator;
import io.dingodb.exec.operator.CoalesceOperator;
import io.dingodb.exec.operator.FilterOperator;
import io.dingodb.exec.operator.GetByKeysOperator;
import io.dingodb.exec.operator.HashJoinOperator;
import io.dingodb.exec.operator.HashOperator;
import io.dingodb.exec.operator.LikeScanOperator;
import io.dingodb.exec.operator.PartCountOperator;
import io.dingodb.exec.operator.PartDeleteOperator;
import io.dingodb.exec.operator.PartInsertOperator;
import io.dingodb.exec.operator.PartRangeDeleteOperator;
import io.dingodb.exec.operator.PartRangeScanOperator;
import io.dingodb.exec.operator.PartScanOperator;
import io.dingodb.exec.operator.PartUpdateOperator;
import io.dingodb.exec.operator.PartitionOperator;
import io.dingodb.exec.operator.ProjectOperator;
import io.dingodb.exec.operator.ReceiveOperator;
import io.dingodb.exec.operator.ReduceOperator;
import io.dingodb.exec.operator.RemovePartOperator;
import io.dingodb.exec.operator.RootOperator;
import io.dingodb.exec.operator.SendOperator;
import io.dingodb.exec.operator.SortOperator;
import io.dingodb.exec.operator.SumUpOperator;
import io.dingodb.exec.operator.ValuesOperator;
import io.dingodb.exec.operator.data.SortCollation;
import io.dingodb.exec.operator.data.SortDirection;
import io.dingodb.exec.operator.data.SortNullDirection;
import io.dingodb.exec.operator.hash.HashStrategy;
import io.dingodb.exec.operator.hash.SimpleHashStrategy;
import io.dingodb.meta.Part;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.common.util.Utils.sole;

@Slf4j
public class DingoJobVisitor implements DingoRelVisitor<Collection<Output>> {
    private final IdGenerator idGenerator;
    private final Location currentLocation;
    private final MetaCache metaCache;
    @Getter
    private final Job job;

    private DingoJobVisitor(Job job, IdGenerator idGenerator, Location currentLocation) {
        this.job = job;
        this.idGenerator = idGenerator;
        this.currentLocation = currentLocation;
        this.metaCache = new MetaCache();
    }

    public static void renderJob(Job job, RelNode input, Location currentLocation) {
        renderJob(job, input, currentLocation, false);
    }

    public static void renderJob(Job job, RelNode input, Location currentLocation, boolean checkRoot) {
        MetaCache.initTableDefinitions();
        IdGenerator idGenerator = new DingoIdGenerator();
        DingoJobVisitor visitor = new DingoJobVisitor(job, idGenerator, currentLocation);
        Collection<Output> outputs = dingo(input).accept(visitor);
        if (checkRoot && outputs.size() > 0) {
            throw new IllegalStateException("There root of plan must be `DingoRoot`.");
        }
        if (log.isDebugEnabled()) {
            log.info("job = {}", job);
        }
    }

    private static @NonNull SortCollation toSortCollation(@NonNull RelFieldCollation collation) {
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

    private static @NonNull TupleMapping getAggKeys(@NonNull ImmutableBitSet groupSet) {
        return TupleMapping.of(
            groupSet.asList().stream()
                .mapToInt(Integer::intValue)
                .toArray()
        );
    }

    private static List<Agg> getAggList(
        @NonNull List<AggregateCall> aggregateCallList,
        DingoType schema
    ) {
        return aggregateCallList.stream()
            .map(c -> AggFactory.getAgg(
                c.getAggregation().getKind(),
                c.getArgList(),
                schema
            ))
            .collect(Collectors.toList());
    }

    public static List<Object[]> getTuplesFromKeyItems(
        @NonNull Collection<Map<Integer, RexLiteral>> items,
        @NonNull TableDefinition td
    ) {
        final TupleMapping revMapping = td.getRevKeyMapping();
        return items.stream()
            .map(item -> {
                Object[] tuple = new Object[item.size()];
                for (Map.Entry<Integer, RexLiteral> entry : item.entrySet()) {
                    tuple[revMapping.get(entry.getKey())] = RexLiteralUtils.convertFromRexLiteral(
                        entry.getValue(),
                        td.getColumn(entry.getKey()).getType()
                    );
                }
                return tuple;
            })
            .collect(Collectors.toList());
    }

    private static @NonNull Map<Location, List<String>> groupAllPartKeysByAddress(
        final @NonNull NavigableMap<ComparableByteArray, Part> parts
    ) {
        Map<Location, List<String>> groupStartKeysByAddress = new HashMap<>();
        for (Map.Entry<ComparableByteArray, Part> entry : parts.entrySet()) {
            Part part = entry.getValue();
            List<String> startKeys = groupStartKeysByAddress.computeIfAbsent(part.getLeader(), k -> new ArrayList<>());
            byte[] keyBytes = entry.getKey().getBytes();
            String startKeyBase64 = ByteArrayUtils.enCodeBytes2Base64(keyBytes);
            startKeys.add(startKeyBase64);
            groupStartKeysByAddress.put(part.getLeader(), startKeys);
            log.info("group start keys in partion by host:{}, keyBytes:{}, startKeyBase64:{}",
                part.getLeader(),
                Arrays.toString(keyBytes),
                startKeyBase64);
        }

        groupStartKeysByAddress.forEach((k, v) -> {
            StringBuilder builder = new StringBuilder();
            v.forEach(s -> {
                byte[] bytes = ByteArrayUtils.deCodeBase64String2Bytes(s);
                String result = Arrays.toString(bytes);
                builder.append(result).append(",");
            });

            log.info("group all part start keys by address:{} startKeys:{}",
                k.toString(),
                builder);
        });
        return groupStartKeysByAddress;
    }

    private @NonNull List<Output> coalesce(@NonNull Collection<Output> inputs) {
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

    private Output exchange(@NonNull Output input, @NonNull Location target, DingoType schema) {
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

    private @NonNull Collection<Output> bridge(@NonNull Collection<Output> inputs,
                                               Supplier<Operator> operatorSupplier) {
        List<Output> outputs = new LinkedList<>();
        for (Output input : inputs) {
            Operator operator = operatorSupplier.get();
            Task task = input.getTask();
            operator.setId(idGenerator.get());
            task.putOperator(operator);
            input.setLink(operator.getInput(0));
            operator.getSoleOutput().copyHint(input);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    private @NonNull Collection<Output> partition(
        @NonNull Collection<Output> inputs,
        @NonNull DingoRelPartitionByTable partition
    ) {
        List<Output> outputs = new LinkedList<>();
        String tableName = MetaCache.getTableName(partition.getTable());
        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> parts = this.metaCache.getParts(tableName);
        final TableDefinition td = this.metaCache.getTableDefinition(tableName);
        final PartitionStrategy<ComparableByteArray> ps = new RangeStrategy(td, parts.navigableKeySet());
        for (Output input : inputs) {
            Task task = input.getTask();
            PartitionOperator operator = new PartitionOperator(
                ps,
                td.getKeyMapping()
            );
            operator.setId(idGenerator.get());
            operator.createOutputs(parts);
            task.putOperator(operator);
            input.setLink(operator.getInput(0));
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    private @NonNull Collection<Output> hash(
        @NonNull Collection<Output> inputs,
        @NonNull DingoRelPartitionByKeys hash
    ) {
        List<Output> outputs = new LinkedList<>();
        final Collection<Location> locations = Services.CLUSTER.getComputingLocations();
        final HashStrategy hs = new SimpleHashStrategy();
        for (Output input : inputs) {
            Task task = input.getTask();
            HashOperator operator = new HashOperator(hs, TupleMapping.of(hash.getKeys()));
            operator.setId(idGenerator.get());
            operator.createOutputs(locations);
            task.putOperator(operator);
            input.setLink(operator.getInput(0));
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    public @NonNull Collection<Output> convertStreaming(
        @NonNull Collection<Output> inputs,
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
        Collection<Output> outputs = inputs;
        if (media.getPartitions().size() > srcPartitions.size()) {
            for (DingoRelPartition partition : media.getPartitions()) {
                if (!srcPartitions.contains(partition)) {
                    if (partition instanceof DingoRelPartitionByTable) {
                        outputs = partition(outputs, (DingoRelPartitionByTable) partition);
                    } else if (partition instanceof DingoRelPartitionByKeys) {
                        outputs = hash(outputs, (DingoRelPartitionByKeys) partition);
                    } else {
                        throw new IllegalStateException("Not supported.");
                    }
                }
            }
        }
        if (!Objects.equals(dstDistribution, srcDistribution)) {
            outputs = outputs.stream().map(input -> {
                Location targetLocation = (dstDistribution == null ? currentLocation : input.getTargetLocation());
                return exchange(input, targetLocation, schema);
            }).collect(Collectors.toList());
        }
        if (dstPartitions.size() < media.getPartitions().size()) {
            assert dstDistribution == null && dstPartitions.size() == 0 || dstPartitions.size() == 1;
            outputs = coalesce(outputs);
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@NonNull DingoAggregate rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        return bridge(inputs, () -> new AggregateOperator(
            getAggKeys(rel.getGroupSet()),
            getAggList(
                rel.getAggCallList(),
                DefinitionMapper.mapToDingoType(rel.getInput().getRowType())
            )
        ));
    }

    @Override
    public Collection<Output> visit(@NonNull DingoFilter rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        RexNode condition = rel.getCondition();
        return bridge(inputs, () -> new FilterOperator(
            SqlExprUtils.toSqlExpr(condition),
            DefinitionMapper.mapToDingoType(rel.getInput().getRowType())
        ));
    }

    @Override
    public Collection<Output> visit(@NonNull DingoGetByKeys rel) {
        String tableName = MetaCache.getTableName(rel.getTable());
        final NavigableMap<ComparableByteArray, Part> parts = this.metaCache.getParts(tableName);
        final TableDefinition td = this.metaCache.getTableDefinition(tableName);
        final CommonId tableId = this.metaCache.getTableId(tableName);
        final PartitionStrategy<ComparableByteArray> ps = new RangeStrategy(td, parts.navigableKeySet());
        List<Object[]> keyTuples = getTuplesFromKeyItems(rel.getKeyItems(), td);
        Map<ComparableByteArray, List<Object[]>> partMap = ps.partKeyTuples(keyTuples);
        List<Output> outputs = new LinkedList<>();
        for (Map.Entry<ComparableByteArray, List<Object[]>> entry : partMap.entrySet()) {
            GetByKeysOperator operator = new GetByKeysOperator(
                tableId,
                entry.getKey(),
                td.getDingoType(),
                td.getKeyMapping(),
                entry.getValue(),
                SqlExprUtils.toSqlExpr(rel.getFilter()),
                rel.getSelection()
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(parts.get(entry.getKey()).getLeader(), idGenerator);
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@NonNull DingoHashJoin rel) {
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
            JoinInfo joinInfo = rel.analyzeCondition();
            Operator operator = new HashJoinOperator(
                TupleMapping.of(joinInfo.leftKeys),
                TupleMapping.of(joinInfo.rightKeys),
                rel.getLeft().getRowType().getFieldCount(),
                rel.getRight().getRowType().getFieldCount(),
                rel.getJoinType() == JoinRelType.LEFT || rel.getJoinType() == JoinRelType.FULL,
                rel.getJoinType() == JoinRelType.RIGHT || rel.getJoinType() == JoinRelType.FULL
            );
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
    public Collection<Output> visit(@NonNull DingoTableModify rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        List<Output> outputs = new LinkedList<>();
        String tableName = MetaCache.getTableName(rel.getTable());
        TableDefinition td = this.metaCache.getTableDefinition(tableName);
        final CommonId tableId = this.metaCache.getTableId(tableName);
        for (Output input : inputs) {
            Task task = input.getTask();
            Operator operator;
            switch (rel.getOperation()) {
                case INSERT:
                    operator = new PartInsertOperator(
                        tableId,
                        input.getHint().getPartId(),
                        td.getDingoType(),
                        td.getKeyMapping()
                    );
                    break;
                case UPDATE:
                    operator = new PartUpdateOperator(
                        tableId,
                        input.getHint().getPartId(),
                        td.getDingoType(),
                        td.getKeyMapping(),
                        TupleMapping.of(td.getColumnIndices(rel.getUpdateColumnList())),
                        rel.getSourceExpressionList().stream()
                            .map(SqlExprUtils::toSqlExpr)
                            .collect(Collectors.toList())
                    );
                    break;
                case DELETE:
                    operator = new PartDeleteOperator(
                        tableId,
                        input.getHint().getPartId(),
                        td.getDingoType(),
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
    public Collection<Output> visit(@NonNull DingoProject rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        return bridge(inputs, () -> new ProjectOperator(
            SqlExprUtils.toSqlExprList(rel.getProjects(), rel.getRowType()),
            DefinitionMapper.mapToDingoType(rel.getInput().getRowType())
        ));
    }

    @Override
    public Collection<Output> visit(@NonNull DingoReduce rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        Operator operator;
        operator = new ReduceOperator(
            getAggKeys(rel.getGroupSet()),
            getAggList(
                rel.getAggregateCallList(),
                DefinitionMapper.mapToDingoType(rel.getOriginalInputType())
            )
        );
        operator.setId(idGenerator.get());
        Output input = sole(inputs);
        Task task = input.getTask();
        task.putOperator(operator);
        input.setLink(operator.getInput(0));
        return operator.getOutputs();
    }

    @Override
    public Collection<Output> visit(@NonNull DingoRoot rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        if (inputs.size() != 1) {
            throw new IllegalStateException("There must be one input to job root.");
        }
        Output input = sole(inputs);
        Operator operator = new RootOperator(DefinitionMapper.mapToDingoType(rel.getRowType()));
        Task task = input.getTask();
        operator.setId(idGenerator.get());
        task.putOperator(operator);
        input.setLink(operator.getInput(0));
        return ImmutableList.of();
    }

    @Override
    public Collection<Output> visit(@NonNull DingoSort rel) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(this);
        return bridge(inputs, () -> new SortOperator(
            rel.getCollation().getFieldCollations().stream()
                .map(DingoJobVisitor::toSortCollation)
                .collect(Collectors.toList()),
            rel.fetch == null ? -1 : RexLiteral.intValue(rel.fetch),
            rel.offset == null ? 0 : RexLiteral.intValue(rel.offset)
        ));
    }

    @Override
    public Collection<Output> visit(@NonNull DingoStreamingConverter rel) {
        return convertStreaming(
            dingo(rel.getInput()).accept(this),
            dingo(rel.getInput()).getStreaming(),
            rel.getStreaming(),
            DefinitionMapper.mapToDingoType(rel.getRowType())
        );
    }

    @Override
    public Collection<Output> visit(@NonNull DingoTableScan rel) {
        String tableName = MetaCache.getTableName(rel.getTable());
        TableDefinition td = this.metaCache.getTableDefinition(tableName);
        List<Location> distributes = this.metaCache.getDistributes(tableName);
        CommonId tableId = this.metaCache.getTableId(tableName);
        List<Output> outputs = new ArrayList<>(distributes.size());
        RexNode filter = rel.getFilter();
        for (int i = 0; i < distributes.size(); i++) {
            PartScanOperator operator = new PartScanOperator(
                tableId,
                i,
                td.getDingoType(),
                td.getKeyMapping(),
                filter != null ? SqlExprUtils.toSqlExpr(filter) : null,
                rel.getSelection()
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(distributes.get(i), idGenerator);
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@NonNull DingoUnion rel) {
        Collection<Output> inputs = new LinkedList<>();
        for (RelNode node : rel.getInputs()) {
            inputs.addAll(dingo(node).accept(this));
        }
        return coalesce(inputs);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoValues rel) {
        DingoRelStreaming streaming = rel.getStreaming();
        if (streaming.equals(DingoRelStreaming.ROOT)) {
            Task task = job.getOrCreate(currentLocation, idGenerator);
            ValuesOperator operator = new ValuesOperator(
                rel.getTuples(),
                Objects.requireNonNull(DefinitionMapper.mapToDingoType(rel.getRowType()))
            );
            operator.setId(idGenerator.get());
            task.putOperator(operator);
            return operator.getOutputs();
        }
        DingoRelPartition distribution = streaming.getDistribution();
        if (distribution instanceof DingoRelPartitionByTable) {
            List<Output> outputs = new LinkedList<>();
            String tableName = MetaCache.getTableName(((DingoRelPartitionByTable) distribution).getTable());
            final NavigableMap<ComparableByteArray, Part> parts = this.metaCache.getParts(tableName);
            final TableDefinition td = this.metaCache.getTableDefinition(tableName);
            final PartitionStrategy<ComparableByteArray> ps = new RangeStrategy(td, parts.navigableKeySet());
            Map<ComparableByteArray, List<Object[]>> partMap = ps.partTuples(
                rel.getTuples(),
                td.getKeyMapping()
            );
            for (Map.Entry<ComparableByteArray, List<Object[]>> entry : partMap.entrySet()) {
                ValuesOperator operator = new ValuesOperator(
                    entry.getValue(),
                    Objects.requireNonNull(DefinitionMapper.mapToDingoType(rel.getRowType()))
                );
                operator.setId(idGenerator.get());
                OutputHint hint = new OutputHint();
                hint.setPartId(entry.getKey());
                Location location = parts.get(entry.getKey()).getLeader();
                hint.setLocation(location);
                operator.getSoleOutput().setHint(hint);
                Task task = job.getOrCreate(location, idGenerator);
                task.putOperator(operator);
                outputs.addAll(operator.getOutputs());
            }
            return outputs;
        }
        throw new IllegalArgumentException("Unsupported streaming \"" + streaming + "\" of values.");
    }

    @Override
    public Collection<Output> visit(@NonNull DingoPartCountDelete rel) {
        String tableName = MetaCache.getTableName(rel.getTable());
        CommonId tableId = this.metaCache.getTableId(tableName);
        TableDefinition td = this.metaCache.getTableDefinition(tableName);
        NavigableMap<ComparableByteArray, Part> parts = this.metaCache.getParts(tableName);
        List<Location> distributeNodes = this.metaCache.getDistributes(tableName);

        List<Output> outputs = new ArrayList<>(distributeNodes.size());
        Map<Location, List<String>> groupAllPartKeysByAddress = groupAllPartKeysByAddress(parts);
        for (Location node : distributeNodes) {
            Operator operator = rel.isDoDeleting() ? new RemovePartOperator(
                tableId,
                groupAllPartKeysByAddress.get(node),
                td.getDingoType(),
                td.getKeyMapping()
            ) : new PartCountOperator(
                tableId,
                groupAllPartKeysByAddress.get(node),
                td.getDingoType(),
                td.getKeyMapping()
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(node, idGenerator);
            task.putOperator(operator);
            OutputHint hint = new OutputHint();
            hint.setToSumUp(true);
            operator.getSoleOutput().setHint(hint);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@NonNull DingoPartRangeScan rel) {
        String tableName = MetaCache.getTableName(rel.getTable());
        TableDefinition td = this.metaCache.getTableDefinition(tableName);
        SqlExpr filter = null;
        if (rel.getFilter() != null) {
            filter = SqlExprUtils.toSqlExpr(rel.getFilter());
        }

        NavigableMap<ComparableByteArray, Part> parts = this.metaCache.getParts(tableName);
        byte[] startKey = rel.getStartKey();
        byte[] endKey = rel.getEndKey();

        ComparableByteArray startByteArray = parts.floorKey(new ComparableByteArray(startKey));
        if (!rel.isNotBetween() && startByteArray == null) {
            log.warn("Get part from table:{} by startKey:{}, result is null", td.getName(), startKey);
            return null;
        }

        // Get all ranges that need to be queried
        Map<byte[], byte[]> allRangeMap = new TreeMap<>(ByteArrayUtils::compare);
        if (rel.isNotBetween()) {
            allRangeMap.put(parts.firstKey().getBytes(), startKey);
            allRangeMap.put(endKey, parts.lastKey().getBytes());
        } else {
            allRangeMap.put(startKey, endKey);
        }

        List<Output> outputs = new ArrayList<>();

        Iterator<Map.Entry<byte[], byte[]>> allRangeIterator = allRangeMap.entrySet().iterator();
        while (allRangeIterator.hasNext()) {
            Map.Entry<byte[], byte[]> entry = allRangeIterator.next();
            startKey = entry.getKey();
            endKey = entry.getValue();

            // Get all partitions based on startKey and endKey
            final PartitionStrategy<ComparableByteArray> ps = new RangeStrategy(td, parts.navigableKeySet());
            Map<byte[], byte[]> partMap = ps.calcPartitionRange(startKey, endKey, rel.isIncludeEnd());

            Iterator<Map.Entry<byte[], byte[]>> partIterator = partMap.entrySet().iterator();
            boolean includeStart = rel.isIncludeStart();
            while (partIterator.hasNext()) {
                Map.Entry<byte[], byte[]> next = partIterator.next();
                PartRangeScanOperator operator = new PartRangeScanOperator(
                    this.metaCache.getTableId(tableName),
                    next.getKey(),
                    td.getDingoType(),
                    td.getKeyMapping(),
                    filter,
                    rel.getSelection(),
                    next.getKey(),
                    next.getValue(),
                    includeStart,
                    next.getValue() != null && rel.isIncludeEnd(),
                    false
                );
                operator.setId(idGenerator.get());
                Task task = job.getOrCreate(
                    parts.get(parts.floorKey(new ComparableByteArray(next.getKey()))).getLeader(), idGenerator
                );
                task.putOperator(operator);
                outputs.addAll(operator.getOutputs());
                includeStart = true;
            }
        }

        return outputs;
    }

    @Override
    public Collection<Output> visit(@NonNull DingoPartRangeDelete rel) {
        String tableName = MetaCache.getTableName(rel.getTable());
        CommonId tableId = this.metaCache.getTableId(tableName);
        TableDefinition td = this.metaCache.getTableDefinition(tableName);
        List<Output> outputs = new ArrayList<>();

        NavigableMap<ComparableByteArray, Part> parts = this.metaCache.getParts(tableName);
        // Get all partitions based on startKey and endKey
        final PartitionStrategy<ComparableByteArray> ps = new RangeStrategy(td, parts.navigableKeySet());
        Map<byte[], byte[]> partMap = ps.calcPartitionRange(rel.getStartKey(), rel.getEndKey(), false);
        Iterator<Map.Entry<byte[], byte[]>> partIterator = partMap.entrySet().iterator();
        while (partIterator.hasNext()) {
            Map.Entry<byte[], byte[]> next = partIterator.next();
            PartRangeDeleteOperator operator = new PartRangeDeleteOperator(
                tableId,
                td.getDingoType(),
                td.getKeyMapping(),
                next.getKey(),
                next.getValue(),
                rel.isIncludeStart(),
                rel.isIncludeEnd()
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(
                parts.get(parts.floorKey(new ComparableByteArray(next.getKey()))).getLeader(), idGenerator
            );
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

    @Override
    public Collection<Output> visit(@NonNull DingoLikeScan rel) {
        String tableName = MetaCache.getTableName(rel.getTable());
        TableDefinition td = this.metaCache.getTableDefinition(tableName);
        SqlExpr filter = null;
        if (rel.getFilter() != null) {
            filter = SqlExprUtils.toSqlExpr(rel.getFilter());
        }
        NavigableMap<ComparableByteArray, Part> parts = this.metaCache.getParts(tableName);
        final PartitionStrategy<ComparableByteArray> ps = new RangeStrategy(td, parts.navigableKeySet());
        Map<byte[], byte[]> prefixRange = ps.calcPartitionByPrefix(rel.getPrefix());
        List<Output> outputs = new ArrayList<>();

        Iterator<Map.Entry<byte[], byte[]>> partIterator = prefixRange.entrySet().iterator();
        while (partIterator.hasNext()) {
            Map.Entry<byte[], byte[]> next = partIterator.next();
            LikeScanOperator operator = new LikeScanOperator(
                this.metaCache.getTableId(tableName),
                next.getKey(),
                td.getDingoType(),
                td.getKeyMapping(),
                filter,
                rel.getSelection(),
                rel.getPrefix()
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(
                parts.get(parts.floorKey(new ComparableByteArray(next.getKey()))).getLeader(), idGenerator
            );
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }
        return outputs;
    }

}
