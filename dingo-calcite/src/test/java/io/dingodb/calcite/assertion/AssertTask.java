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

package io.dingodb.calcite.assertion;

import io.dingodb.common.Location;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.SourceOperator;

import java.util.List;
import java.util.function.Consumer;
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
        assertThat(instance.getOperators()).size().isEqualTo(num);
        return this;
    }

    public AssertTask sourceNum(int num) {
        List<Id> runList = instance.getRunList();
        assertThat(runList).size().isEqualTo(num);
        return this;
    }

    @Nonnull
    public AssertOperator soleSource() {
        List<Id> runList = instance.getRunList();
        assertThat(runList).size().isEqualTo(1);
        return Assert.operator(instance.getOperator(sole(runList)));
    }

    @Nonnull
    public AssertTask source(int index, @Nonnull Consumer<AssertOperator> consumer) {
        List<Id> runList = instance.getRunList();
        assertThat(runList).size().isGreaterThan(index);
        AssertOperator assertOperator = Assert.operator(instance.getOperator(runList.get(index)))
            .isA(SourceOperator.class);
        consumer.accept(assertOperator);
        return this;
    }
}
