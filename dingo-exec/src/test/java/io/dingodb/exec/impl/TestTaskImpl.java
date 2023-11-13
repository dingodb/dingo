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
import io.dingodb.common.Location;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.CommonId;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.ProjectOperator;
import io.dingodb.exec.operator.RootOperator;
import io.dingodb.exec.operator.ValuesOperator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTaskImpl {
    @Test
    public void testValues() {
        Task task = new TaskImpl(CommonId.EMPTY_TASK, CommonId.EMPTY_JOB, Mockito.mock(Location.class), null);
        ValuesOperator values = new ValuesOperator(
            ImmutableList.of(
                new Object[]{1, "Alice", 1.0},
                new Object[]{2, "Betty", 2.0}
            ),
            DingoTypeFactory.tuple("INTEGER", "STRING", "DOUBLE")
        );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl(CommonId.EMPTY_JOB.seq);
        values.setId(idGenerator.getOperatorId(CommonId.EMPTY_TASK.seq));
        task.putOperator(values);
        RootOperator root = new RootOperator(DingoTypeFactory.tuple("INTEGER", "STRING", "DOUBLE"), null);
        root.setId(idGenerator.getOperatorId(CommonId.EMPTY_TASK.seq));
        task.putOperator(root);
        values.getOutputs().get(0).setLink(root.getInput(0));
        task.init();
        task.run(null);
        assertThat(root.popValue()).containsExactly(1, "Alice", 1.0);
        assertThat(root.popValue()).containsExactly(2, "Betty", 2.0);
    }

    @Test
    public void testParas() {
        DingoType parasType = DingoTypeFactory.tuple("INT", "STRING");
        Task task = new TaskImpl(CommonId.EMPTY_TASK, CommonId.EMPTY_JOB, Mockito.mock(Location.class), parasType);
        ValuesOperator values = new ValuesOperator(
            ImmutableList.of(new Object[]{0}),
            DingoTypeFactory.tuple("INT")
        );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl(CommonId.EMPTY_JOB.seq);
        values.setId(idGenerator.getOperatorId(CommonId.EMPTY_TASK.seq));
        task.putOperator(values);
        ProjectOperator project = new ProjectOperator(
            Arrays.asList(
                new SqlExpr("_P[0]", DingoTypeFactory.scalar("INT")),
                new SqlExpr("_P[1]", DingoTypeFactory.scalar("STRING"))
            ),
            DingoTypeFactory.tuple("INT")
        );
        project.setId(idGenerator.getOperatorId(CommonId.EMPTY_TASK.seq));
        task.putOperator(project);
        RootOperator root = new RootOperator(DingoTypeFactory.tuple("INTEGER", "STRING"), null);
        root.setId(idGenerator.getOperatorId(CommonId.EMPTY_TASK.seq));
        task.putOperator(root);
        values.getSoleOutput().setLink(project.getInput(0));
        project.getSoleOutput().setLink(root.getInput(0));
        task.init();
        task.run(new Object[]{1, "Alice"});
        assertThat(root.popValue()).containsExactly(1, "Alice");
        while (root.popValue() != RootOperator.FIN) {
            root.popValue();
        }
        task.run(new Object[]{2, "Betty"});
        assertThat(root.popValue()).containsExactly(2, "Betty");
        while (root.popValue() != RootOperator.FIN) {
            root.popValue();
        }
    }
}
