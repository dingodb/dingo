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
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.expr.json.runtime.Parser;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.tso.TsoService;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;

@JsonPropertyOrder({"tasks"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class JobImpl implements Job {
    public static final Parser PARSER = Parser.JSON;

    @JsonProperty("jobId")
    @Getter
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private CommonId jobId;

    @JsonProperty("txnId")
    @Getter
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private CommonId txnId;

    @Getter
    @JsonProperty("tasks")
    @JsonSerialize(keyUsing = CommonId.JacksonKeySerializer.class, contentAs = TaskImpl.class)
    @JsonDeserialize(keyUsing = CommonId.JacksonKeyDeserializer.class, contentAs = TaskImpl.class)
    private final Map<CommonId, Task> tasks;

    // Need not serialize this, for it is serialized in tasks.
    private final DingoType parasType;

    private CommonId rootTaskId = null;

    private final long maxExecutionTime;
    private final Boolean isSelect;

    @JsonCreator
    public JobImpl(@JsonProperty("jobId") CommonId jobId, @JsonProperty("jobId") CommonId txnId) {
        this(jobId, txnId,null, 0, null);
    }

    public JobImpl(@JsonProperty("jobId") CommonId jobId,
                   @JsonProperty("jobId") CommonId txnId,
                   @Nullable DingoType parasType,
                   @JsonProperty("executeTimeout") long maxExecutionTimeout,
                   @JsonProperty("isSelect") Boolean isSelect) {
        this.jobId = jobId;
        this.txnId = txnId;
        this.tasks = new HashMap<>();
        this.parasType = parasType;
        this.maxExecutionTime = maxExecutionTimeout;
        this.isSelect = isSelect;
    }

    @Override
    public @NonNull Task create(CommonId id,
                                Location location,
                                TransactionType transactionType,
                                IsolationLevel isolationLevel) {
        if (tasks.containsKey(id)) {
            throw new IllegalArgumentException("The task \"" + id + "\" already exists in job \"" + jobId + "\".");
        }
        Task task = new TaskImpl(
            id,
            jobId,
            txnId,
            location,
            parasType,
            transactionType,
            isolationLevel,
            maxExecutionTime,
            isSelect);
        tasks.put(id, task);
        return task;
    }

    @Nullable
    public DingoType getParasType() {
        return parasType;
    }

    @Override
    public Task getRoot() {
        return tasks.get(rootTaskId);
    }

    @Override
    public void markRoot(CommonId taskId) {
        assert tasks.get(taskId).getRoot() != null
            : "The root task must has a root operator.";
        rootTaskId = taskId;
    }

    @Override
    public String toString() {
        try {
            return PARSER.stringify(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setTxnId(CommonId txnId) {
        this.txnId = txnId;
        long jobSeqId = TsoService.getDefault().tso();
        this.jobId = new CommonId(CommonId.CommonType.JOB, txnId.seq, jobSeqId);
        getTasks().values().forEach( v -> v.setTxnId(txnId));
        getTasks().values().forEach( v -> v.setBathTask(true));
    }
}
