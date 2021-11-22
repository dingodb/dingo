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
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.AbstractOperator;
import io.dingodb.exec.operator.RootOperator;
import io.dingodb.exec.operator.SourceOperator;
import io.dingodb.net.Location;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;

@Slf4j
@JsonPropertyOrder({"jobId", "location", "operators", "runList"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class TaskImpl implements Task {
    @JsonProperty("jobId")
    @Getter
    private final String jobId;
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

    @JsonCreator
    public TaskImpl(
        @JsonProperty("jobId") String jobId,
        @JsonProperty("operators") Map<Id, Operator> operators,
        @JsonProperty("runList") List<Id> runList,
        @JsonProperty("location") Location location
    ) {
        this.jobId = jobId;
        this.location = location;
        this.operators = operators;
        this.runList = runList;
    }

    TaskImpl(String jobId, Location location) {
        this(jobId, new HashMap<>(), new LinkedList<>(), location);
    }

    public static TaskImpl deserialize(String str) throws JsonProcessingException {
        return JobImpl.PARSER.parse(str, TaskImpl.class);
    }

    @Override
    public RootOperator getRoot() {
        return operators.values().stream()
            .filter(o -> o instanceof RootOperator)
            .map(o -> (RootOperator) o)
            .findFirst()
            .orElse(null);
    }

    @Override
    public void putOperator(@Nonnull Operator operator) {
        operator.setTask(this);
        operators.put(operator.getId(), operator);
        if (operator instanceof SourceOperator) {
            runList.add(operator.getId());
        }
    }

    @Override
    public void deleteOperator(@Nonnull Operator operator) {
        operators.remove(operator.getId());
        runList.remove(operator.getId());
    }

    public void run() {
        log.info("Task is starting at {}...", location);
        ExecutorService executorService = Executors.newFixedThreadPool(runList.size());
        runList.forEach(id -> {
            final Operator operator = operators.get(id);
            assert operator instanceof SourceOperator
                : "Operators in run list must be source operator.";
            executorService.execute(() -> {
                try {
                    while (operator.push(0, null)) {
                        log.info("Operator {} need another pushing.", operator.getId());
                    }
                } catch (RuntimeException e) {
                    e.printStackTrace();
                }
            });
        });
    }

    @Nonnull
    @Override
    public byte[] serialize() {
        return toString().getBytes(StandardCharsets.UTF_8);
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
