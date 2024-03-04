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
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.OperatorFactory;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Status;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.ErrorType;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.TaskStatus;
import io.dingodb.exec.operator.SourceOperator;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.dingodb.exec.utils.OperatorCodeUtils.ROOT;
import static io.dingodb.exec.utils.OperatorCodeUtils.SOURCE;

@Slf4j
@JsonPropertyOrder({"txnType", "isolationLevel", "txnId" , "jobId",
    "location", "operators", "runList", "parasType", "bachTask"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class TaskImpl implements Task {
    @JsonProperty("id")
    @Getter
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId id;
    @JsonProperty("jobId")
    @Getter
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId jobId;
    @JsonProperty("txnId")
    @Getter
    @Setter
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private CommonId txnId;
    @JsonProperty("location")
    @Getter
    private final Location location;
    @Getter
    @JsonProperty("vertexes")
    @JsonSerialize(keyUsing = CommonId.JacksonKeySerializer.class, contentAs = Vertex.class)
    @JsonDeserialize(keyUsing = CommonId.JacksonKeyDeserializer.class, contentAs = Vertex.class)
    private final Map<CommonId, Vertex> vertexes;
    @JsonProperty("runList")
    @Getter
    @JsonSerialize(contentUsing = CommonId.JacksonSerializer.class)
    @JsonDeserialize(contentUsing = CommonId.JacksonDeserializer.class)
    private final List<CommonId> runList;
    @JsonProperty("parasType")
    @Getter
    private final DingoType parasType;
    @JsonProperty("txnType")
    @Getter
    private final TransactionType transactionType;
    @JsonProperty("isolationLevel")
    @Getter
    private final IsolationLevel isolationLevel;
    @JsonProperty("bachTask")
    private boolean bachTask;

    @JsonProperty("maxExecutionTime")
    private long maxExecutionTime;

    @JsonProperty("isSelect")
    private Boolean isSelect;
    private CommonId rootOperatorId = null;

    private transient AtomicInteger status = new AtomicInteger(Status.BORN);
    private transient CountDownLatch activeThreads = null;
    @Getter
    private transient TaskStatus taskInitStatus;
    @Setter
    private transient Context context;

    @JsonCreator
    public TaskImpl(
        @JsonProperty("id") CommonId id,
        @JsonProperty("jobId") CommonId jobId,
        @JsonProperty("txnId") CommonId txnId,
        @JsonProperty("location") Location location,
        @JsonProperty("parasType") DingoType parasType,
        @JsonProperty("txnType") TransactionType transactionType,
        @JsonProperty("isolationLevel") IsolationLevel isolationLevel,
        @JsonProperty("maxExecutionTime") long maxExecutionTime,
        @JsonProperty("isSelect") Boolean isSelect
    ) {
        this.id = id;
        this.jobId = jobId;
        this.txnId = txnId;
        this.location = location;
        this.parasType = parasType;
        this.runList = new LinkedList<>();
        this.vertexes = new HashMap<>();
        this.transactionType = transactionType;
        this.isolationLevel = isolationLevel;
        this.maxExecutionTime = maxExecutionTime;
        this.isSelect = isSelect;
        this.context = Context.builder().pin(0).keyState(new ArrayList<>()).build();
    }

    public Context getContext() {
        if (this.context == null) {
            this.context = Context.builder().pin(0).keyState(new ArrayList<>()).build();
        }
        return this.context;
    }

    @Override
    public Vertex getRoot() {
        return vertexes.get(rootOperatorId);
    }

    @Override
    public void markRoot(CommonId operatorId) {
        assert vertexes.get(operatorId).getOp().equals(ROOT)
            : "The root operator must be a `RootOperator`.";
        rootOperatorId = operatorId;
    }

    @Override
    public int getStatus() {
        return status.get();
    }

    @Override
    public void putVertex(@NonNull Vertex vertex) {
        vertex.setTask(this);
        vertexes.put(vertex.getId(), vertex);
        if (vertex.getOp().domain == SOURCE) {
            runList.add(vertex.getId());
        }
    }

    @Override
    public void init() {
        status = new AtomicInteger(Status.BORN);
        boolean isStatusOK = true;
        String statusErrMsg = "";
        this.getVertexes().forEach((id, v) -> {
            v.setId(id);
            v.setTask(this);
        });

        for (Vertex vertex : this.getVertexes().values()) {
            try {
                vertex.init();
            } catch (Exception ex) {
                log.error("Init operator:{} in task:{} failed catch exception:{}",
                    vertex.getOp(), this.id.toString(), ex, ex);
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
            Vertex vertex = vertexes.get(runList.get(0));
            // Try to propagate error by any operator, may not succeed because task init failed.
            // operator.fin(0, FinWithException.of(taskInitStatus));
            Operator operator = OperatorFactory.getInstance(vertex.getOp());
            operator.fin(0, FinWithException.of(taskInitStatus), vertex);
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
        if (bachTask) {
            setStartTs(txnId.seq);
        }
        for (CommonId operatorId : runList) {
            Vertex vertex = vertexes.get(operatorId);
            Operator operator = OperatorFactory.getInstance(vertex.getOp());
            assert operator instanceof SourceOperator
                : "Operators in run list must be source operator.";
            Executors.execute("operator-" + jobId + "-" + id + "-" + operatorId, () -> {
                final long startTime = System.currentTimeMillis();
                try {
                    while (operator.push(getContext().copy(), null, vertex)) {
                        log.info("Operator {} need another pushing.", vertex.getId());
                    }
                    operator.fin(0, null, vertex);
                } catch (RuntimeException e) {
                    log.error("Run Task:{} catch operator:{} run Exception:{}",
                        getId().toString(), vertex.getId(), e, e);
                    TaskStatus taskStatus = new TaskStatus();
                    taskStatus.setStatus(false);
                    taskStatus.setTaskId(vertex.getTask().getId().toString());
                    taskStatus.setErrorMsg(e.toString());
                    if (e instanceof WriteConflictException) {
                        taskStatus.setErrorType(ErrorType.WriteConflict);
                    } else if (e instanceof DuplicateEntryException) {
                        taskStatus.setErrorType(ErrorType.DuplicateEntry);
                    } else {
                        taskStatus.setErrorType(ErrorType.TaskFin);
                    }
                    operator.fin(0, FinWithException.of(taskStatus), vertex);
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
    public void setBathTask(boolean bathTask) {
        this.bachTask = bathTask;
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
