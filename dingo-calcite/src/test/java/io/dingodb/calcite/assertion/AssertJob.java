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

import io.dingodb.common.util.Utils;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

public final class AssertJob extends Assert<Job, AssertJob> {
    AssertJob(Job job) {
        super(job);
    }

    public AssertJob taskNum(int num) {
        assertThat(instance.getTasks()).size().isEqualTo(num);
        return this;
    }

    @Nonnull
    public AssertJob task(Id id, @Nonnull Consumer<AssertTask> consumer) {
        Map<Id, Task> tasks = instance.getTasks();
        consumer.accept(Assert.task(tasks.get(id)));
        return this;
    }

    @Nonnull
    public AssertTask soleTask() {
        Collection<Task> tasks = instance.getTasks().values();
        assertThat(tasks).size().isEqualTo(1);
        return Assert.task(Utils.sole(tasks));
    }
}
