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

import io.dingodb.calcite.rel.DingoRepeatUnion;
import io.dingodb.calcite.rel.dingo.DingoRoot;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.impl.JobManagerImpl;
import io.dingodb.exec.operator.params.RepeatUnionParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.meta.MetaService;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static io.dingodb.exec.utils.OperatorCodeUtils.REPEAT_UNION;

@Slf4j
public class DingoRepeatUnionVisitFun {
    public static @NonNull Collection<Vertex> visit(
        Job job,
        @NonNull IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        ITransaction transaction,
        @NonNull DingoRepeatUnion rel
    ) {
        Job seedJob = getSpoolJob(transaction.getStartTs(), rel.getSeedRel(), transaction);
        Job iterationJob = getSpoolJob(transaction.getStartTs(), rel.getIterativeRel(), transaction);
        RepeatUnionParam repeatUnionParam = new RepeatUnionParam(seedJob, iterationJob, rel.all, rel.iterationLimit);
        Task task = job.getOrCreate(currentLocation, idGenerator);
        Vertex vertex = new Vertex(REPEAT_UNION, repeatUnionParam);
        vertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(vertex);

        List<Vertex> outputs = new ArrayList<>();
        outputs.add(vertex);
        return outputs;
    }

    public static Job getSpoolJob(long startTs, RelNode relNode, ITransaction transaction) {
        RelNode relInput;
        if (relNode instanceof RelSubset) {
            RelSubset relSubset = (RelSubset) relNode;
            relNode = relSubset.getBest();

            List<Integer> selection = new ArrayList<>();
            selection.add(0);
            relInput = new DingoRoot(relNode.getCluster(), relNode.getTraitSet(), relNode, TupleMapping.of(selection));
        } else {
            relInput = relNode;
        }
        JobManager jobManager = JobManagerImpl.INSTANCE;
        long jobSeqId = TsoService.getDefault().cacheTso();
        CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION,
            TransactionManager.getServerId().seq, TransactionManager.getStartTs());
        RelDataType parasType = new RelRecordType(new ArrayList<>());
        Job job = jobManager.createJob(startTs, jobSeqId, txnId, DefinitionMapper.mapToDingoType(parasType));
        Location currentLocation = MetaService.root().currentLocation();
        DingoJobVisitor.renderJob(
            jobManager, job, relInput, currentLocation, false,
            transaction, null, null, 0,
            false, false, false, 1, "root", "%"
        );
        return job;
    }
}
