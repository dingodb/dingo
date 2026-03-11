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
import io.dingodb.calcite.rel.DingoTableSpool;
import io.dingodb.calcite.rel.dingo.DingoRoot;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.ExecuteVariables;
import io.dingodb.common.Location;
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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.dingodb.exec.utils.OperatorCodeUtils.REPEAT_UNION;

@Slf4j
public final class DingoRepeatUnionVisitFun {
    private DingoRepeatUnionVisitFun() {
    }

    public static @NonNull Collection<Vertex> visit(
        Job job,
        @NonNull IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        ITransaction transaction,
        @NonNull DingoRepeatUnion rel,
        ExecuteVariables executeVariables
    ) {
        List<RexNode> rexNodeList = checkUnionType(rel.getSeedRel(), rel.getIterativeRel());
        Job seedJob = getSpoolJob(transaction.getStartTs(), rel, transaction, executeVariables, true, rexNodeList);
        Job iterationJob = getSpoolJob(transaction.getStartTs(), rel,
            transaction, executeVariables, false, rexNodeList);
        RepeatUnionParam repeatUnionParam = new RepeatUnionParam(seedJob, iterationJob,
            rel.all, executeVariables.getIterationLimit());
        Task task = job.getOrCreate(currentLocation, idGenerator);
        Vertex vertex = new Vertex(REPEAT_UNION, repeatUnionParam);
        vertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(vertex);

        List<Vertex> outputs = new ArrayList<>();
        outputs.add(vertex);
        return outputs;
    }

    public static Job getSpoolJob(
        long startTs, DingoRepeatUnion rel,
        ITransaction transaction, ExecuteVariables executeVariables,
        boolean seed,
        List<RexNode> rexNodeList
    ) {
        RelNode relInput;
        if (seed) {
            RelNode relNode = rel.getSeedRel();
            if (relNode instanceof RelSubset) {
                RelSubset relSubset = (RelSubset) relNode;
                relNode = relSubset.getBest();
                if (relNode == null) {
                    throw new RuntimeException("can not get the best rel");
                }
                if (rexNodeList != null && relNode instanceof DingoTableSpool) {
                    DingoTableSpool dingoTableSpool = (DingoTableSpool) relNode;
                    dingoTableSpool.setRexNodeList(rexNodeList);
                    dingoTableSpool.setTargetRowType(rel.getIterativeRel().getRowType());
                }
                relInput = new DingoRoot(relNode.getCluster(), relNode.getTraitSet(), relNode, null);
            } else {
                relInput = relNode;
            }
        } else {
            RelNode relNode = rel.getIterativeRel();
            if (relNode instanceof RelSubset) {
                RelSubset relSubset = (RelSubset) relNode;
                relNode = relSubset.getBest();
                if (relNode == null) {
                    throw new RuntimeException("can not get the best rel");
                }
                relInput = new DingoRoot(relNode.getCluster(), relNode.getTraitSet(), relNode, null);
            } else {
                relInput = relNode;
            }
        }
        JobManager jobManager = JobManagerImpl.INSTANCE;
        long jobSeqId = TsoService.getDefault().cacheTso();
        CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION,
            TransactionManager.getServerId().seq, TransactionManager.getStartTs());
        RelDataType parasType = new RelRecordType(new ArrayList<>());
        Job job = jobManager.createJob(startTs, jobSeqId, txnId,
            DefinitionMapper.mapToDingoType(parasType), executeVariables.getQueryId());
        job.setUser(executeVariables.getUser());
        job.setHost(executeVariables.getHost());
        Location currentLocation = MetaService.root().currentLocation();

        DingoJobVisitor.renderJob(
            jobManager, job, relInput, currentLocation, false,
            transaction, null, executeVariables, 0,
            false, false, false, 1, "root", "%"
        );
        return job;
    }

    public static List<RexNode> checkUnionType(RelNode seedRel, RelNode iterativeRel) {
        RelDataType relDataType1 = seedRel.getRowType();
        RelDataType relDataType2 = iterativeRel.getRowType();
        if (relDataType1.getFieldCount() != relDataType2.getFieldCount()) {
            return null;
        }
        List<RexNode> rexNodeList = new ArrayList<>();
        int diffCnt = 0;
        for (int i = 0; i < relDataType1.getFieldCount(); i ++) {
            RelDataTypeField typeField1 = relDataType1.getFieldList().get(i);
            RelDataTypeField typeField2 = relDataType2.getFieldList().get(i);
            if (typeField1.getType() != typeField2.getType()) {
                RelDataType targetType = typeField2.getType();
                RexInputRef inputRef = new RexInputRef(i, typeField1.getType());
                List<RexNode> operands = new ArrayList<>();
                operands.add(inputRef);
                RexNode cast = new RexCall(targetType, new SqlCastFunction(), operands);
                rexNodeList.add(cast);
                diffCnt ++;
            } else {
                rexNodeList.add(new RexInputRef(i, typeField1.getType()));
            }
        }
        if (diffCnt == 0) {
            return null;
        }
        return rexNodeList;
    }
}
