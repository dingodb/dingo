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

import io.dingodb.common.CommonId;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.base.TaskManager;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public final class TaskManagerImpl implements TaskManager {
    public static final TaskManagerImpl INSTANCE = new TaskManagerImpl();

    private final Map<CommonId, Task> taskMap = new ConcurrentHashMap<>();

    private TaskManagerImpl() {
    }

    public static @NonNull String taskFullId(CommonId jobId, CommonId taskId) {
        return jobId + ":" + taskId;
    }

    @Override
    public void addTask(@NonNull Task task) {
        CommonId jobId = task.getJobId();
        CommonId id = task.getId();
        String taskFullId = taskFullId(jobId, id);
        task.init();
        taskMap.put(id, task);
        if (log.isDebugEnabled()) {
            log.debug("Added task \"{}\". # of job:tasks: {}.", taskFullId, taskMap.size());
        }
    }

    @Override
    public @NonNull Task getTask(CommonId jobId, CommonId taskId) {
        String taskFullId = taskFullId(jobId, taskId);
        Task task = taskMap.get(taskId);
        if (task != null) {
            return task;
        }
        throw new IllegalArgumentException("Non-existed job:task id \"" + taskFullId + "\".");
    }

    @Override
    public void removeTask(CommonId jobId, CommonId taskId) {
        String taskFullId = taskFullId(jobId, taskId);
        Task task = taskMap.remove(taskId);
        if (log.isDebugEnabled()) {
            log.debug("Removed task \"{}\". # of job:tasks: {}.", taskFullId, taskMap.size());
        }
        if (task != null) {
            task.destroy();
        }
    }

    @Override
    public void close() {
        taskMap.values().forEach(Task::destroy);
    }
}
