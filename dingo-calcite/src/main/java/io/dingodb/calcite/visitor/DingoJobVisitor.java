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
import io.dingodb.calcite.rel.DingoFilter;
import io.dingodb.calcite.rel.DingoGetByIndex;
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
import io.dingodb.calcite.traits.DingoRelPartitionByKeys;
import io.dingodb.calcite.traits.DingoRelPartitionByTable;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.function.DingoAggregateVisitFun;
import io.dingodb.calcite.visitor.function.DingoCoalesce;
import io.dingodb.calcite.visitor.function.DingoCountDeleteVisitFun;
import io.dingodb.calcite.visitor.function.DingoFilterVisitFun;
import io.dingodb.calcite.visitor.function.DingoGetByIndexVisitFun;
import io.dingodb.calcite.visitor.function.DingoGetByKeysFun;
import io.dingodb.calcite.visitor.function.DingoHashJoinVisitFun;
import io.dingodb.calcite.visitor.function.DingoLikeScanVisitFun;
import io.dingodb.calcite.visitor.function.DingoProjectVisitFun;
import io.dingodb.calcite.visitor.function.DingoRangeDeleteVisitFun;
import io.dingodb.calcite.visitor.function.DingoRangeScanVisitFun;
import io.dingodb.calcite.visitor.function.DingoReduceVisitFun;
import io.dingodb.calcite.visitor.function.DingoRootVisitFun;
import io.dingodb.calcite.visitor.function.DingoSortVisitFun;
import io.dingodb.calcite.visitor.function.DingoStreamingConverterVisitFun;
import io.dingodb.calcite.visitor.function.DingoTableModifyVisitFun;
import io.dingodb.calcite.visitor.function.DingoUnionVisitFun;
import io.dingodb.calcite.visitor.function.DingoValuesVisitFun;
import io.dingodb.cluster.ClusterService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.impl.IdGeneratorImpl;
import io.dingodb.exec.operator.HashOperator;
import io.dingodb.exec.operator.PartitionOperator;
import io.dingodb.exec.operator.hash.HashStrategy;
import io.dingodb.exec.operator.hash.SimpleHashStrategy;
import io.dingodb.exec.partition.PartitionStrategy;
import io.dingodb.exec.partition.RangeStrategy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.calcite.rel.DingoRel.dingo;

@Slf4j
public class DingoJobVisitor implements DingoRelVisitor<Collection<Output>> {
    private final IdGenerator idGenerator;
    private final Location currentLocation;
    @Getter
    private final Job job;

    private DingoJobVisitor(Job job, IdGenerator idGenerator, Location currentLocation) {
        this.job = job;
        this.idGenerator = idGenerator;
        this.currentLocation = currentLocation;
    }

    public static void renderJob(Job job, RelNode input, Location currentLocation) {
        renderJob(job, input, currentLocation, false);
    }

    public static void renderJob(Job job, RelNode input, Location currentLocation, boolean checkRoot) {
        IdGenerator idGenerator = new IdGeneratorImpl();
        DingoJobVisitor visitor = new DingoJobVisitor(job, idGenerator, currentLocation);
        Collection<Output> outputs = dingo(input).accept(visitor);
        if (checkRoot && outputs.size() > 0) {
            throw new IllegalStateException("There root of plan must be `DingoRoot`.");
        }
        if (log.isDebugEnabled()) {
            log.info("job = {}", job);
        }
    }

    @Override
    public Collection<Output> visit(@NonNull DingoStreamingConverter rel) {
        return DingoStreamingConverterVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoAggregate rel) {
        return DingoAggregateVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoFilter rel) {
        return DingoFilterVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoHashJoin rel) {
        return DingoHashJoinVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoTableModify rel) {
        return DingoTableModifyVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoProject rel) {
        return DingoProjectVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoReduce rel) {
        return DingoReduceVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoRoot rel) {
        return DingoRootVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoGetByIndex rel) {
        return DingoGetByIndexVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoGetByKeys rel) {
        return DingoGetByKeysFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoSort rel) {
        return DingoSortVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoTableScan rel) {
        // current version scan must have range
        return visit(DingoPartRangeScan.of(rel));
    }

    @Override
    public Collection<Output> visit(@NonNull DingoUnion rel) {
        return DingoUnionVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoValues rel) {
        return DingoValuesVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoPartCountDelete rel) {
        return DingoCountDeleteVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoPartRangeScan rel) {
        return DingoRangeScanVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoPartRangeDelete rel) {
        return DingoRangeDeleteVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Output> visit(@NonNull DingoLikeScan rel) {
        return DingoLikeScanVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

}
