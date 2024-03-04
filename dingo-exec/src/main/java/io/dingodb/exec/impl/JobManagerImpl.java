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
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.base.Status;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.base.TaskManager;
import io.dingodb.exec.impl.message.CreateTaskMessage;
import io.dingodb.exec.impl.message.DestroyTaskMessage;
import io.dingodb.exec.impl.message.RunTaskMessage;
import io.dingodb.exec.impl.message.TaskMessage;
import io.dingodb.exec.operator.params.RootParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.meta.MetaService;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public final class JobManagerImpl implements JobManager {
    public static final String TASK_TAG = "DINGO_TASK";
    public static final JobManagerImpl INSTANCE = new JobManagerImpl(10);

    private final Map<CommonId, Job> jobMap = new ConcurrentHashMap<>();
    private final Map<Location, Channel> channelMap;
    @Getter
    private final TaskManager taskManager;
    private final IdGenerator idGenerator;

    private JobManagerImpl(int capacity) {
        channelMap = new ConcurrentHashMap<>(capacity);
        taskManager = TaskManagerImpl.INSTANCE;
        idGenerator = new IdGeneratorImpl();
    }

    @Override
    public @NonNull Job createJob(long startTs,
                                  long jobSeqId,
                                  CommonId txnId,
                                  DingoType parasType,
                                  long maxExecutionTime,
                                  Boolean isSelect) {
        Job job = new JobImpl(idGenerator.getJobId(startTs, jobSeqId), txnId, parasType, maxExecutionTime, isSelect);
        CommonId jobId = job.getJobId();
        jobMap.put(jobId, job);
        if (log.isDebugEnabled()) {
            log.debug("Created job \"{}\". # of jobs: {}.", jobId, jobMap.size());
        }
        return job;
    }

    @Override
    public Job getJob(CommonId jobId) {
        return jobMap.get(jobId);
    }

    @Override
    public void removeJob(CommonId jobId) {
        Job job = jobMap.remove(jobId);
        if (log.isDebugEnabled()) {
            log.debug("Removed job \"{}\". # of jobs: {}.", jobId, jobMap.size());
        }
        if (job != null) {
            for (Task task : job.getTasks().values()) {
                if (task.getRoot() != null) {
                    taskManager.removeTask(task);
                    continue;
                }
                sendTaskMessage(task, new Message(TASK_TAG, new DestroyTaskMessage(task).toBytes()));
            }
        }
    }

    @Override
    public @NonNull Iterator<Object[]> createIterator(@NonNull Job job, Object @Nullable [] paras) {
        if (job.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (job.getStatus() == Status.BORN) {
            distributeTasks(job);
        }
        run(job, paras);
        Task root = job.getRoot();
        return new JobIteratorImpl(job, root.getRoot());
    }

    @Override
    public @NonNull Iterator<Object[]> createIterator(@NonNull Job job, Object @Nullable [] paras, long takeNextTimeout) {
        RootParam param = job.getRoot().getRoot().getParam();
        param.setTakeTtl(takeNextTimeout);
        return createIterator(job, paras);
    }

    @Override
    public void close() {
        channelMap.values().forEach(Channel::close);
        jobMap.keySet().forEach(this::removeJob);
        taskManager.close();
    }

    private void distributeTasks(@NonNull Job job) {
        for (Task task : job.getTasks().values()) {
            if (task.getRoot() != null) {
                assert task.getLocation().equals(MetaService.root().currentLocation())
                    : "The root task must be at current location.";
                taskManager.addTask(task);
                continue;
            }
            // Currently only root task is run at localhost, if a task is at localhost but not root task,
            // it is just ignored. Just distribute all the tasks to avoid this.
            try {
                sendTaskMessage(task, new Message(TASK_TAG, new CreateTaskMessage(task).toBytes()));
            } catch (Exception e) {
                log.error("Error to distribute tasks.", e);
                throw new RuntimeException("Error to distribute tasks.", e);
            }
        }
    }

    private void run(@NonNull Job job, Object @Nullable [] paras) {
        for (Task task : job.getTasks().values()) {
            if (task.getRoot() != null) {
                task.run(paras);
                continue;
            }
            sendTaskMessage(task, new Message(TASK_TAG, new RunTaskMessage(task, job.getParasType(), paras).toBytes()));
        }
    }

    private void sendTaskMessage(@NonNull Task task, Message message) {
        Location location = task.getLocation();
        Channel channel = channelMap.computeIfAbsent(
            location,
            l -> Services.openNewSysChannel(l.getHost(), l.getPort())
        );
        channel.setCloseListener(__ -> channelMap.remove(location));
        channel.send(message);
        ITransaction transaction = TransactionManager.getTransaction(task.getTxnId());
        if (transaction != null) {
            transaction.registerChannel(task.getId(), channel);
        }
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
            // 1、cross node need add transaction
            // 2、check whether the current node can execute transactions
            ITransaction transaction = TransactionManager.getTransaction(task.getTxnId() == null ? CommonId.EMPTY_TRANSACTION : task.getTxnId());
            if (transaction == null) {
                TransactionManager.createTransaction(task.getTransactionType(),
                    task.getTxnId() == null ? CommonId.EMPTY_TRANSACTION : task.getTxnId(),
                    task.getIsolationLevel().getCode());
            }
            taskManager.addTask(task);
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
        taskManager.removeTask(cmd.getJobId(), cmd.getTaskId());
    }
}
