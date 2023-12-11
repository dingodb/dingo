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
import io.dingodb.calcite.rel.DingoFunctionScan;
import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.rel.DingoGetByIndexMerge;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoGetVectorByDistance;
import io.dingodb.calcite.rel.DingoHashJoin;
import io.dingodb.calcite.rel.DingoLikeScan;
import io.dingodb.calcite.rel.DingoPartCountDelete;
import io.dingodb.calcite.rel.DingoPartRangeDelete;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoReduce;
import io.dingodb.calcite.rel.DingoRoot;
import io.dingodb.calcite.rel.DingoSort;
import io.dingodb.calcite.rel.DingoStreamingConverter;
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.DingoUnion;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.calcite.rel.DingoVector;
import io.dingodb.calcite.rel.VectorStreamConvertor;
import io.dingodb.calcite.visitor.function.DingoAggregateVisitFun;
import io.dingodb.calcite.visitor.function.DingoCountDeleteVisitFun;
import io.dingodb.calcite.visitor.function.DingoFilterVisitFun;
import io.dingodb.calcite.visitor.function.DingoFunctionScanVisitFun;
import io.dingodb.calcite.visitor.function.DingoGetByIndexMergeVisitFun;
import io.dingodb.calcite.visitor.function.DingoGetByIndexVisitFun;
import io.dingodb.calcite.visitor.function.DingoGetByKeysFun;
import io.dingodb.calcite.visitor.function.DingoGetVectorByDistanceVisitFun;
import io.dingodb.calcite.visitor.function.DingoHashJoinVisitFun;
import io.dingodb.calcite.visitor.function.DingoLikeScanVisitFun;
import io.dingodb.calcite.visitor.function.DingoProjectVisitFun;
import io.dingodb.calcite.visitor.function.DingoRangeDeleteVisitFun;
import io.dingodb.calcite.visitor.function.DingoReduceVisitFun;
import io.dingodb.calcite.visitor.function.DingoRootVisitFun;
import io.dingodb.calcite.visitor.function.DingoSortVisitFun;
import io.dingodb.calcite.visitor.function.DingoStreamingConverterVisitFun;
import io.dingodb.calcite.visitor.function.DingoTableModifyVisitFun;
import io.dingodb.calcite.visitor.function.DingoTableScanVisitFun;
import io.dingodb.calcite.visitor.function.DingoUnionVisitFun;
import io.dingodb.calcite.visitor.function.DingoValuesVisitFun;
import io.dingodb.calcite.visitor.function.DingoVectorStreamingVisitFun;
import io.dingodb.calcite.visitor.function.DingoVectorVisitFun;
import io.dingodb.common.Location;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.impl.IdGeneratorImpl;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;

import static io.dingodb.calcite.rel.DingoRel.dingo;

@Slf4j
public class DingoJobVisitor implements DingoRelVisitor<Collection<Vertex>> {
    private final IdGenerator idGenerator;
    private final Location currentLocation;
    @Getter
    private final Job job;
    private final boolean isTxn;

    private DingoJobVisitor(Job job, IdGenerator idGenerator, Location currentLocation, boolean isTxn) {
        this.job = job;
        this.idGenerator = idGenerator;
        this.currentLocation = currentLocation;
        this.isTxn = isTxn;
    }

    public static void renderJob(Job job, RelNode input, Location currentLocation) {
        renderJob(job, input, currentLocation, false, false);
    }

    public static void renderJob(Job job, RelNode input, Location currentLocation, boolean checkRoot, boolean isTxn) {
        IdGenerator idGenerator = new IdGeneratorImpl(job.getJobId().seq);
        DingoJobVisitor visitor = new DingoJobVisitor(job, idGenerator, currentLocation, isTxn);
        Collection<Vertex> outputs = dingo(input).accept(visitor);
        if (checkRoot && outputs.size() > 0) {
            throw new IllegalStateException("There root of plan must be `DingoRoot`.");
        }
        if (log.isDebugEnabled()) {
            log.info("job = {}", job);
        }
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoStreamingConverter rel) {
        return DingoStreamingConverterVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoAggregate rel) {
        return DingoAggregateVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoFilter rel) {
        return DingoFilterVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoHashJoin rel) {
        return DingoHashJoinVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoTableModify rel) {
        return DingoTableModifyVisitFun.visit(job, idGenerator, currentLocation, isTxn, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoProject rel) {
        return DingoProjectVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoReduce rel) {
        return DingoReduceVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoRoot rel) {
        return DingoRootVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoGetByIndex rel) {
        return DingoGetByIndexVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoGetByKeys rel) {
        return DingoGetByKeysFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoSort rel) {
        return DingoSortVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoTableScan rel) {
        // current version scan must have range
        return DingoTableScanVisitFun.visit(job, idGenerator, currentLocation, isTxn, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoUnion rel) {
        return DingoUnionVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoValues rel) {
        return DingoValuesVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoPartCountDelete rel) {
        return DingoCountDeleteVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoPartRangeDelete rel) {
        return DingoRangeDeleteVisitFun.visit(job, idGenerator, currentLocation, isTxn, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoLikeScan rel) {
        return DingoLikeScanVisitFun.visit(job, idGenerator, currentLocation, isTxn, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoFunctionScan rel) {
        return DingoFunctionScanVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoVector rel) {
        return DingoVectorVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }


    public Collection<Vertex> visit(@NonNull DingoGetVectorByDistance rel) {
        return DingoGetVectorByDistanceVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull VectorStreamConvertor rel) {
        return DingoVectorStreamingVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }

    @Override
    public Collection<Vertex> visit(@NonNull DingoGetByIndexMerge rel) {
        return DingoGetByIndexMergeVisitFun.visit(job, idGenerator, currentLocation, this, rel);
    }
}
