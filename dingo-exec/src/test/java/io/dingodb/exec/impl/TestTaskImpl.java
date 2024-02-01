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

package io.dingodb.exec.impl;

import com.google.common.collect.ImmutableList;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.exec.OperatorFactory;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.RootOperator;
import io.dingodb.exec.operator.params.ProjectParam;
import io.dingodb.exec.operator.params.RootParam;
import io.dingodb.exec.operator.params.ValuesParam;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static io.dingodb.exec.utils.OperatorCodeUtils.PROJECT;
import static io.dingodb.exec.utils.OperatorCodeUtils.ROOT;
import static io.dingodb.exec.utils.OperatorCodeUtils.VALUES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTaskImpl {
    @Test
    public void testValues() {
        Task task = new TaskImpl(CommonId.EMPTY_TASK, CommonId.EMPTY_JOB, CommonId.EMPTY_TRANSACTION, Mockito.mock(Location.class), null,
            TransactionType.OPTIMISTIC, IsolationLevel.SnapshotIsolation, 0, null);
        ValuesParam param = new ValuesParam(
            ImmutableList.of(
                new Object[]{1, "Alice", 1.0},
                new Object[]{2, "Betty", 2.0}
            ),
            DingoTypeFactory.INSTANCE.tuple("INTEGER", "STRING", "DOUBLE")
        );
        Vertex values = new Vertex(VALUES, param);
        IdGeneratorImpl idGenerator = new IdGeneratorImpl(CommonId.EMPTY_JOB.seq);
        values.setId(idGenerator.getOperatorId(CommonId.EMPTY_TASK.seq));
        task.putVertex(values);
        RootParam rootParam = new RootParam(DingoTypeFactory.INSTANCE.tuple("INTEGER", "STRING", "DOUBLE"), null);
        Vertex root = new Vertex(ROOT, rootParam);
        CommonId id = idGenerator.getOperatorId(CommonId.EMPTY_TASK.seq);
        root.setId(id);
        task.putVertex(root);
        task.markRoot(id);
        Edge edge = new Edge(values, root);
        values.addEdge(edge);
        root.addIn(edge);
        task.init();
        task.run(null);

        RootOperator rootOperator = (RootOperator) OperatorFactory.getInstance(task.getRoot().getOp());
        assertThat(rootOperator.popValue(root)).containsExactly(1, "Alice", 1.0);
        assertThat(rootOperator.popValue(root)).containsExactly(2, "Betty", 2.0);
    }

    @Test
    public void testParas() {
        DingoType parasType = DingoTypeFactory.INSTANCE.tuple("INT", "STRING");
        Task task = new TaskImpl(CommonId.EMPTY_TASK, CommonId.EMPTY_JOB, CommonId.EMPTY_TRANSACTION, Mockito.mock(Location.class), parasType,
            TransactionType.OPTIMISTIC, IsolationLevel.SnapshotIsolation, 0, null);
        ValuesParam valuesParam = new ValuesParam(
            ImmutableList.of(new Object[]{0}),
            DingoTypeFactory.INSTANCE.tuple("INT")
        );
        Vertex values = new Vertex(VALUES, valuesParam);
        IdGeneratorImpl idGenerator = new IdGeneratorImpl(CommonId.EMPTY_JOB.seq);
        values.setId(idGenerator.getOperatorId(CommonId.EMPTY_TASK.seq));
        task.putVertex(values);
        ProjectParam projectParam = new ProjectParam(
            Arrays.asList(
                new SqlExpr("_P[0]", DingoTypeFactory.INSTANCE.scalar("INT")),
                new SqlExpr("_P[1]", DingoTypeFactory.INSTANCE.scalar("STRING"))
            ),
            DingoTypeFactory.INSTANCE.tuple("INT")
        );
        Vertex project = new Vertex(PROJECT, projectParam);
        project.setId(idGenerator.getOperatorId(CommonId.EMPTY_TASK.seq));
        task.putVertex(project);
        Edge valuesEdge = new Edge(values, project);
        values.addEdge(valuesEdge);
        project.addIn(valuesEdge);
        RootParam rootParam = new RootParam(DingoTypeFactory.INSTANCE.tuple("INTEGER", "STRING"), null);
        Vertex root = new Vertex(ROOT, rootParam);
        CommonId id = idGenerator.getOperatorId(CommonId.EMPTY_TASK.seq);
        root.setId(id);
        task.putVertex(root);
        task.markRoot(id);
        Edge projectEdge = new Edge(project, root);
        project.addEdge(projectEdge);
        root.addIn(projectEdge);
        task.init();
        task.run(new Object[]{1, "Alice"});
        RootOperator rootOperator = (RootOperator) OperatorFactory.getInstance(task.getRoot().getOp());
        assertThat(rootOperator.popValue(root)).containsExactly(1, "Alice");
        while (rootOperator.popValue(root) != RootOperator.FIN) {
            rootOperator.popValue(root);
        }
        task.run(new Object[]{2, "Betty"});
        assertThat(rootOperator.popValue(root)).containsExactly(2, "Betty");
        while (rootOperator.popValue(root) != RootOperator.FIN) {
            rootOperator.popValue(root);
        }
    }
}
