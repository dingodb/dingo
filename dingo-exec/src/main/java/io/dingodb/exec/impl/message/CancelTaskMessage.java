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

package io.dingodb.exec.impl.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.exec.base.Task;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

@JsonTypeName("cancel")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CancelTaskMessage extends TaskMessage {
    @JsonProperty("job")
    @Getter
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId jobId;
    @JsonProperty("task")
    @Getter
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId taskId;

    public CancelTaskMessage(
        CommonId jobId,
        CommonId taskId
    ) {
        this.jobId = jobId;
        this.taskId = taskId;
    }

    public CancelTaskMessage(
        @NonNull Task task
    ) {
        this(task.getJobId(), task.getId());
    }

    @JsonCreator
    public static @NonNull CancelTaskMessage fromJson(
        @JsonProperty("job") CommonId jobId,
        @JsonProperty("task") CommonId taskId
    ) {
        return new CancelTaskMessage(jobId, taskId);
    }

}
