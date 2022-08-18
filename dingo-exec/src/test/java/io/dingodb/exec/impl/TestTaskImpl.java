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
import com.google.common.collect.ImmutableMap;
import io.dingodb.common.Location;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.expr.SqlParaCompileContext;
import io.dingodb.exec.expr.SqlParasCompileContext;
import io.dingodb.exec.operator.ProjectOperator;
import io.dingodb.exec.operator.RootOperator;
import io.dingodb.exec.operator.ValuesOperator;
import io.dingodb.expr.runtime.TypeCode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTaskImpl {
    @Test
    public void testValues() {
        Task task = new TaskImpl(Id.NULL, Id.NULL, Mockito.mock(Location.class), null);
        ValuesOperator values = new ValuesOperator(
            ImmutableList.of(
                new Object[]{1, "Alice", 1.0},
                new Object[]{2, "Betty", 2.0}
            ),
            DingoTypeFactory.tuple("INTEGER", "STRING", "DOUBLE")
        );
        values.setId(new Id("0"));
        task.putOperator(values);
        RootOperator root = new RootOperator(DingoTypeFactory.tuple("INTEGER", "STRING", "DOUBLE"));
        root.setId(new Id("1"));
        task.putOperator(root);
        values.getOutputs().get(0).setLink(root.getInput(0));
        task.init();
        task.run();
        assertThat(root.popValue()).containsExactly(1, "Alice", 1.0);
        assertThat(root.popValue()).containsExactly(2, "Betty", 2.0);
    }

    @Test
    public void testParas() {
        Map<String, SqlParaCompileContext> paraNameTypeMap = new TreeMap<>();
        paraNameTypeMap.put("a", new SqlParaCompileContext(TypeCode.INT));
        paraNameTypeMap.put("b", new SqlParaCompileContext(TypeCode.STRING));
        SqlParasCompileContext parasCompileContext = new SqlParasCompileContext(paraNameTypeMap);
        Task task = new TaskImpl(Id.NULL, Id.NULL, Mockito.mock(Location.class), parasCompileContext);
        ValuesOperator values = new ValuesOperator(
            ImmutableList.of(new Object[]{0}),
            DingoTypeFactory.tuple("INT")
        );
        values.setId(new Id("0"));
        task.putOperator(values);
        ProjectOperator project = new ProjectOperator(
            Arrays.asList(
                new SqlExpr("_P['a']", DingoTypeFactory.scalar("INT")),
                new SqlExpr("_P['b']", DingoTypeFactory.scalar("STRING"))
            ),
            DingoTypeFactory.tuple("INT")
        );
        project.setId(new Id("1"));
        task.putOperator(project);
        RootOperator root = new RootOperator(DingoTypeFactory.tuple("INTEGER", "STRING"));
        root.setId(new Id("2"));
        task.putOperator(root);
        values.getSoleOutput().setLink(project.getInput(0));
        project.getSoleOutput().setLink(root.getInput(0));
        task.init();
        task.setParas(ImmutableMap.of("a", 1, "b", "Alice"));
        task.run();
        assertThat(root.popValue()).containsExactly(1, "Alice");
        while (root.popValue() != RootOperator.FIN) {
            root.popValue();
        }
        task.setParas(ImmutableMap.of("a", 2, "b", "Betty"));
        task.reset();
        task.run();
        assertThat(root.popValue()).containsExactly(2, "Betty");
        while (root.popValue() != RootOperator.FIN) {
            root.popValue();
        }
    }
}
