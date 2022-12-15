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

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.base.TaskManager;
import io.dingodb.exec.impl.message.CreateTaskMessage;
import io.dingodb.exec.impl.message.DestroyTaskMessage;
import io.dingodb.exec.impl.message.RunTaskMessage;
import io.dingodb.exec.impl.message.TaskMessage;
import io.dingodb.net.Message;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public final class TaskMessenger {
    public static final String TASK_TAG = "DINGO_TASK";
    public static final TaskMessenger INSTANCE = new TaskMessenger();

    private final TaskManager taskManager;

    private TaskMessenger() {
        taskManager = TaskManagerImpl.INSTANCE;
    }

    public static @NonNull Message createTaskMessage(
        @NonNull Task task
    ) {
        return new Message(TASK_TAG, new CreateTaskMessage(task).toBytes());
    }

    public static @NonNull Message runTaskMessage(
        @NonNull Id jobId,
        @NonNull Id taskId,
        @NonNull DingoType paraTypes,
        Object @Nullable [] paras
    ) {
        return new Message(TASK_TAG, new RunTaskMessage(jobId, taskId, paraTypes, paras).toBytes());
    }

    public static @NonNull Message destroyTaskMessage(
        @NonNull Id jobId,
        @NonNull Id taskId
    ) {
        return new Message(TASK_TAG, new DestroyTaskMessage(jobId, taskId).toBytes());
    }

    public void processMessage(@NonNull Message message) {
        final Timer.Context timeCtx = DingoMetrics.getTimeContext("deserialize");
        TaskMessage taskMessage;
        try {
            taskMessage = TaskMessage.fromBytes(message.content());
        } catch (JsonProcessingException e) {
            // TODO: sql execution will be hang up.
            throw new RuntimeException("Cannot deserialize received TaskMessage.", e);
        }
        timeCtx.stop();
        if (taskMessage instanceof CreateTaskMessage) {
            processCommand((CreateTaskMessage) taskMessage);
        } else if (taskMessage instanceof RunTaskMessage) {
            processCommand((RunTaskMessage) taskMessage);
        } else if (taskMessage instanceof DestroyTaskMessage) {
            processCommand((DestroyTaskMessage) taskMessage);
        }
    }

    private void processCommand(@NonNull CreateTaskMessage cmd) {
        final long startTime = System.currentTimeMillis();
        try {
            Task task = cmd.getTask();
            taskManager.addTask(task);
            task.init();
        } finally {
            final long cost = System.currentTimeMillis() - startTime;
            if (log.isDebugEnabled()) {
                log.debug("Time cost: {}ms.", cost);
            }
            DingoMetrics.latency("on_task_message", cost);
        }
    }

    private void processCommand(@NonNull RunTaskMessage cmd) {
        Task task = taskManager.getTask(cmd.getJobId(), cmd.getTaskId());
        task.run(cmd.getParas());
    }

    private void processCommand(@NonNull DestroyTaskMessage cmd) {
        Task task = taskManager.removeTask(cmd.getJobId(), cmd.getTaskId());
        if (task != null) {
            task.destroy();
        }
    }
}
