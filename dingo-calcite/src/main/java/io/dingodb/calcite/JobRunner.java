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

package io.dingodb.calcite;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.Location;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.impl.JobImpl;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

import javax.annotation.Nonnull;

import static io.dingodb.exec.Services.TASK_TAG;

@Slf4j
@RequiredArgsConstructor
public final class JobRunner {
    private final Job job;

    @SuppressWarnings("unused")
    @Nonnull
    public static Enumerable<Object[]> run(String serializedJob) {
        try {
            final Job job = JobImpl.fromString(serializedJob);
            final JobRunner runner = new JobRunner(job);
            return new AbstractEnumerable<Object[]>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    return runner.createEnumerator();
                }
            };
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot deserialize job.", e);
        }
    }

    @SuppressWarnings("unused")
    @Nonnull
    public static Enumerable<Object> runOneColumn(String serializedJob) {
        try {
            final Job job = JobImpl.fromString(serializedJob);
            final JobRunner runner = new JobRunner(job);
            return new AbstractEnumerable<Object>() {
                @Override
                public Enumerator<Object> enumerator() {
                    return runner.createEnumeratorSingleColumn();
                }
            };
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot deserialize job.", e);
        }
    }

    @Nonnull
    public Enumerator<Object[]> createEnumerator() {
        if (job.isEmpty()) {
            return Linq4j.emptyEnumerator();
        }
        Task task = distributeTasks();
        assert task.getLocation().equals(Services.META.currentLocation())
            : "The root task must be at current location.";
        task.init();
        task.run();
        return new RootEnumerator(task.getRoot());
    }

    @Nonnull
    public Enumerator<Object> createEnumeratorSingleColumn() {
        return Linq4j.transform(createEnumerator(), (Object[] o) -> o[0]);
    }

    /**
     * Distribute the tasks.
     *
     * @return the root task
     */
    private Task distributeTasks() {
        Task rootTask = null;
        for (Task task : job.getTasks().values()) {
            if (task.getRoot() != null) {
                rootTask = task;
                continue;
            }
            Location location = task.getLocation();
            if (!location.equals(Services.META.currentLocation())) {
                try {
                    Channel channel = Services.openNewSysChannel(location.getHost(), location.getPort());
                    Message msg = Message.builder()
                        .tag(TASK_TAG)
                        .content(task.serialize())
                        .build();
                    channel.send(msg);
                    channel.close();
                } catch (Exception e) {
                    log.error("Error to distribute tasks.", e);
                    throw new RuntimeException("Error to distribute tasks.", e);
                }
            }
        }
        assert rootTask != null : "There must be one and only one root task.";
        return rootTask;
    }
}
