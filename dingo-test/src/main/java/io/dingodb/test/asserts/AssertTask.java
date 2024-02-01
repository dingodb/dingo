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

package io.dingodb.test.asserts;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.exec.OperatorFactory;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.SourceOperator;

import java.util.List;
import javax.annotation.Nonnull;

import static io.dingodb.common.util.Utils.sole;
import static org.assertj.core.api.Assertions.assertThat;

public final class AssertTask extends Assert<Task, AssertTask> {
    AssertTask(Task obj) {
        super(obj);
    }

    public AssertTask location(Location location) {
        assertThat(instance).hasFieldOrPropertyWithValue("location", location);
        return this;
    }

    public AssertTask operatorNum(int num) {
        assertThat(instance.getVertexes()).size().isEqualTo(num);
        return this;
    }

    public AssertTask sourceNum(int num) {
        List<CommonId> runList = instance.getRunList();
        assertThat(runList).size().isEqualTo(num);
        return this;
    }

    @Nonnull
    public AssertOperator soleSource() {
        List<CommonId> runList = instance.getRunList();
        assertThat(runList).size().isEqualTo(1);
        Vertex vertex = instance.getVertex(sole(runList));
        Operator operator = OperatorFactory.getInstance(vertex.getOp());
        return Assert.operator(operator, vertex);
    }

    @Nonnull
    public AssertOperator source(int index) {
        List<CommonId> runList = instance.getRunList();
        assertThat(runList).size().isGreaterThan(index);
        Vertex vertex = instance.getVertex(runList.get(index));
        Operator operator = OperatorFactory.getInstance(vertex.getOp());
        return Assert.operator(operator, vertex)
            .isA(SourceOperator.class);
    }
}
