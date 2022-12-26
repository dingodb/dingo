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

import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.base.TaskManager;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TaskManagerImpl implements TaskManager {
    public static final TaskManagerImpl INSTANCE = new TaskManagerImpl();

    private final Map<Id, Task> taskMap = new ConcurrentHashMap<>();

    private TaskManagerImpl() {
    }

    private static @NonNull Id taskFullId(Id jobId, Id taskId) {
        return new Id(jobId + ":" + taskId);
    }

    @Override
    public void addTask(Task task) {
        taskMap.put(taskFullId(task.getJobId(), task.getId()), task);
    }

    @Override
    public @NonNull Task getTask(Id jobId, Id taskId) {
        Id id = taskFullId(jobId, taskId);
        Task task = taskMap.get(id);
        if (task != null) {
            return task;
        }
        throw new IllegalArgumentException("Non-existed task id \"" + id + "\".");
    }

    @Override
    public Task removeTask(Id jobId, Id taskId) {
        return taskMap.remove(taskFullId(jobId, taskId));
    }
}