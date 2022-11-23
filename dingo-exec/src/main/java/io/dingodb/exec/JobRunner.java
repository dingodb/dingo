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

package io.dingodb.exec;

import io.dingodb.common.Location;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.Iterator;

import static io.dingodb.exec.Services.TASK_TAG;

@Slf4j
@RequiredArgsConstructor
public final class JobRunner {
    private final Job job;

    public @NonNull Iterator<Object[]> createIterator() {
        if (job.isEmpty()) {
            return Collections.emptyIterator();
        }
        Task task = distributeTasks();
        assert task.getLocation().equals(Services.META.currentLocation())
            : "The root task must be at current location.";
        task.init();
        task.run();
        return task.getRoot().getIterator();
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
            // Currently only root task is run at localhost, if a task is at localhost but not root task,
            // it is just ignored. Just distribute all the tasks to avoid this.
            try {
                Channel channel = Services.openNewSysChannel(location.getHost(), location.getPort());
                Message msg = new Message(TASK_TAG, task.serialize());
                channel.send(msg);
                channel.close();
            } catch (Exception e) {
                log.error("Error to distribute tasks.", e);
                throw new RuntimeException("Error to distribute tasks.", e);
            }
        }
        assert rootTask != null : "There must be one and only one root task.";
        return rootTask;
    }
}
