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

import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;

import java.util.List;
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
    public AssertJob task(int index, @Nonnull Consumer<AssertTask> consumer) {
        List<Task> tasks = instance.getTasks();
        assertThat(tasks).size().isGreaterThan(index);
        consumer.accept(Assert.task(tasks.get(index)));
        return this;
    }

    @Nonnull
    public AssertTask soleTask() {
        List<Task> tasks = instance.getTasks();
        assertThat(tasks).size().isEqualTo(1);
        return Assert.task(tasks.get(0));
    }
}
