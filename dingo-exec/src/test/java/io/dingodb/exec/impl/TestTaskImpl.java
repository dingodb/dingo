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
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.RootOperator;
import io.dingodb.exec.operator.ValuesOperator;
import io.dingodb.meta.Location;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTaskImpl {
    private static final IdGenerator idGenerator = new IdGenerator();

    @Test
    public void testValues() {
        Task task = new TaskImpl("", Mockito.mock(Location.class));
        ValuesOperator values = new ValuesOperator(
            ImmutableList.of(
                new Object[]{1, "Alice", 1.0},
                new Object[]{2, "Betty", 2.0}
            )
        );
        values.setId(idGenerator.get());
        task.putOperator(values);
        RootOperator root = new RootOperator(TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"));
        root.setId(idGenerator.get());
        task.putOperator(root);
        values.getOutputs().get(0).setLink(root.getInput(0));
        task.init();
        task.run();
        assertThat(root.popValue()).containsExactly(1, "Alice", 1.0);
        task.run();
        assertThat(root.popValue()).containsExactly(2, "Betty", 2.0);
    }
}
