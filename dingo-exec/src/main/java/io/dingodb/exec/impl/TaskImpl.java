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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Status;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.TaskStatus;
import io.dingodb.exec.operator.AbstractOperator;
import io.dingodb.exec.operator.RootOperator;
import io.dingodb.exec.operator.SourceOperator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@JsonPropertyOrder({"jobId", "location", "operators", "runList", "parasType"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class TaskImpl implements Task {
    @JsonProperty("id")
    @Getter
    private final Id id;
    @JsonProperty("jobId")
    @Getter
    private final Id jobId;
    @JsonProperty("location")
    @Getter
    private final Location location;
    @JsonProperty("operators")
    @JsonSerialize(contentAs = AbstractOperator.class)
    @JsonDeserialize(contentAs = AbstractOperator.class)
    @Getter
    private final Map<Id, Operator> operators;
    @JsonProperty("runList")
    @Getter
    private final List<Id> runList;
    @JsonProperty("parasType")
    @Getter
    private final DingoType parasType;
    private final transient AtomicInteger status;
    private Id rootOperatorId = null;
    private CountDownLatch activeThreads = null;
    @Getter
    private TaskStatus taskInitStatus;

    @JsonCreator
    public TaskImpl(
        @JsonProperty("id") Id id,
        @JsonProperty("jobId") Id jobId,
        @JsonProperty("location") Location location,
        @JsonProperty("parasType") DingoType parasType
    ) {
        this.id = id;
        this.jobId = jobId;
        this.location = location;
        this.parasType = parasType;
        this.operators = new HashMap<>();
        this.runList = new LinkedList<>();
        this.status = new AtomicInteger(Status.BORN);
    }

    @Override
    public Operator getRoot() {
        return operators.get(rootOperatorId);
    }

    @Override
    public void markRoot(Id operatorId) {
        assert operators.get(operatorId) instanceof RootOperator
            : "The root operator must be a `RootOperator`.";
        rootOperatorId = operatorId;
    }

    @Override
    public int getStatus() {
        return status.get();
    }

    @Override
    public void putOperator(@NonNull Operator operator) {
        operator.setTask(this);
        operators.put(operator.getId(), operator);
        if (operator instanceof SourceOperator) {
            runList.add(operator.getId());
        }
    }

    @Override
    public void init() {
        boolean isStatusOK = true;
        String statusErrMsg = "";
        getOperators().forEach((id, o) -> {
            o.setId(id);
            o.setTask(this);
        });

        for (Operator operator : getOperators().values()) {
            try {
                operator.init();
            } catch (Exception ex) {
                log.error("Init operator:{} in task:{} failed catch exception:{}",
                    operator.toString(), this.id.toString(), ex, ex);
                statusErrMsg = ex.toString();
                isStatusOK = false;
            }
        }
        taskInitStatus = new TaskStatus();
        taskInitStatus.setStatus(isStatusOK);
        taskInitStatus.setTaskId(this.id.toString());
        taskInitStatus.setErrorMsg(statusErrMsg);
        if (taskInitStatus.getStatus()) {
            status.compareAndSet(Status.BORN, Status.READY);
        }
    }

    @Override
    public void run(Object @Nullable [] paras) {
        if (status.get() == Status.BORN) {
            log.error("Run task but check task has init failed: {}", taskInitStatus);
            final Operator operator = operators.get(runList.get(0));
            // Try to propagate error by any operator, may not succeed because task init failed.
            operator.fin(0, FinWithException.of(taskInitStatus));
            return;
        }
        // This method should not be blocked, so schedule a running thread.
        Executors.execute("task-" + jobId + "-" + id, () -> internalRun(paras));
    }

    // Synchronize to make sure there are only one thread run this.
    private synchronized void internalRun(Object @Nullable [] paras) {
        if (!status.compareAndSet(Status.READY, Status.RUNNING)) {
            throw new RuntimeException("Status should be READY.");
        }
        if (log.isDebugEnabled()) {
            log.debug("Task {}-{} is starting at {}...", jobId, id, location);
        }
        activeThreads = new CountDownLatch(runList.size());
        setParas(paras);
        for (Id operatorId : runList) {
            final Operator operator = operators.get(operatorId);
            assert operator instanceof SourceOperator
                : "Operators in run list must be source operator.";
            Executors.execute("operator-" + jobId + "-" + id + "-" + operatorId, () -> {
                final long startTime = System.currentTimeMillis();
                try {
                    while (operator.push(0, null)) {
                        log.info("Operator {} need another pushing.", operator.getId());
                    }
                    operator.fin(0, null);
                } catch (RuntimeException e) {
                    log.error("Run Task:{} catch operator:{} run Exception:{}",
                        getId().toString(), operator.getId(), e, e);
                    TaskStatus taskStatus = new TaskStatus();
                    taskStatus.setStatus(false);
                    taskStatus.setTaskId(operator.getTask().getId().toString());
                    taskStatus.setErrorMsg(e.toString());
                    operator.fin(0, FinWithException.of(taskStatus));
                } finally {
                    if (log.isDebugEnabled()) {
                        log.debug("TaskImpl run cost: {}ms.", System.currentTimeMillis() - startTime);
                    }
                    activeThreads.countDown();
                }
            });
        }
        while (true) {
            try {
                activeThreads.await();
                break;
            } catch (InterruptedException ignored) {
            }
        }
        status.compareAndSet(Status.RUNNING, Status.READY);
        status.compareAndSet(Status.STOPPED, Status.READY);
    }

    @Override
    public boolean cancel() {
        return status.compareAndSet(Status.RUNNING, Status.STOPPED);
    }

    @Override
    public String toString() {
        try {
            return JobImpl.PARSER.stringify(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
