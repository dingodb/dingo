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

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoPartRangeDelete;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.DistributionSourceParam;
import io.dingodb.exec.operator.params.PartRangeDeleteParam;
import io.dingodb.exec.operator.params.TxnPartRangeDeleteParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.transaction.data.IsolationLevel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.exec.utils.OperatorCodeUtils.CALC_DISTRIBUTION;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_RANGE_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_RANGE_DELETE;

public final class DingoRangeDeleteVisitFun {

    private DingoRangeDeleteVisitFun() {
    }

    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation,
        ITransaction transaction, DingoJobVisitor visitor, DingoPartRangeDelete rel
    ) {
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(rel.getTable());
        final Table td = rel.getTable().unwrap(DingoTable.class).getTable();
        NavigableMap<ComparableByteArray, RangeDistribution> ranges = tableInfo.getRangeDistributions();
        DistributionSourceParam distributionParam = new DistributionSourceParam(
            td,
            ranges,
            rel.getStartKey(),
            rel.getEndKey(),
            rel.isIncludeStart(),
            rel.isIncludeEnd(),
            null,
            false,
            rel.isNotBetween(),
            null);
        Vertex calcVertex = new Vertex(CALC_DISTRIBUTION, distributionParam);
        Task task;
        if (transaction != null) {
            task = job.getOrCreate(
                currentLocation,
                idGenerator,
                transaction.getType(),
                IsolationLevel.of(transaction.getIsolationLevel())
            );
        } else {
            task = job.getOrCreate(currentLocation, idGenerator);
        }
        calcVertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(calcVertex);

        List<Vertex> outputs = new ArrayList<>();

        for (int i = 0; i <= Optional.mapOrGet(td.getPartitions(), List::size, () -> 0); i++) {
            Vertex deleteVertex;
            if (transaction != null) {
                TxnPartRangeDeleteParam param = new TxnPartRangeDeleteParam(
                    tableInfo.getId(),
                    td.tupleType(),
                    td.keyMapping());
                deleteVertex = new Vertex(TXN_PART_RANGE_DELETE, param);
            } else {
                PartRangeDeleteParam param = new PartRangeDeleteParam(
                    tableInfo.getId(),
                    td.tupleType(),
                    td.keyMapping());
                deleteVertex = new Vertex(PART_RANGE_DELETE, param);
            }
            if (transaction != null) {
                task = job.getOrCreate(
                    currentLocation,
                    idGenerator,
                    transaction.getType(),
                    IsolationLevel.of(transaction.getIsolationLevel())
                );
            } else {
                task = job.getOrCreate(currentLocation, idGenerator);
            }
            deleteVertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(deleteVertex);
            OutputHint outputHint = new OutputHint();
            outputHint.setToSumUp(true);
            deleteVertex.setHint(outputHint);
            Edge edge = new Edge(calcVertex, deleteVertex);
            calcVertex.addEdge(edge);
            deleteVertex.addIn(edge);
            outputs.add(deleteVertex);
        }

        return outputs;
    }
}
