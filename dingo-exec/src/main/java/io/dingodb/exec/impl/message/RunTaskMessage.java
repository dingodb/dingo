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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.CommonId;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.codec.RawJsonDeserializer;
import io.dingodb.exec.converter.JsonConverter;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@JsonTypeName("run")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RunTaskMessage extends TaskMessage {
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
    @JsonProperty("parasType")
    @Getter
    private final @NonNull DingoType parasType;
    @Getter
    private final Object @Nullable [] paras;

    public RunTaskMessage(
        CommonId jobId,
        CommonId taskId,
        @NonNull DingoType parasType,
        Object @Nullable [] paras
    ) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.parasType = parasType;
        this.paras = paras;
    }

    public RunTaskMessage(
        @NonNull Task task,
        @NonNull DingoType parasType,
        Object @Nullable [] paras
    ) {
        this(task.getJobId(), task.getId(), parasType, paras);
    }

    @JsonCreator
    public static @NonNull RunTaskMessage fromJson(
        @JsonProperty("job") CommonId jobId,
        @JsonProperty("task") CommonId taskId,
        @NonNull @JsonProperty("parasType") DingoType parasType,
        @JsonDeserialize(using = RawJsonDeserializer.class)
        @JsonProperty("paras") JsonNode paras
    ) {
        Object[] newParas = null;
        if (paras != null) {
            newParas = (Object[]) parasType.convertFrom(paras, JsonConverter.INSTANCE);
        }
        return new RunTaskMessage(jobId, taskId, parasType, newParas);
    }

    @JsonProperty("paras")
    Object @Nullable [] getParasJson() {
        return (Object[]) parasType.convertTo(paras, JsonConverter.INSTANCE);
    }
}
