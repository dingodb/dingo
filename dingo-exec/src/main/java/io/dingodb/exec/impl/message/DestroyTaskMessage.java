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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Task;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

@JsonTypeName("destroy")
public class DestroyTaskMessage extends TaskMessage {
    @JsonProperty("job")
    @Getter
    private final Id jobId;
    @JsonProperty("task")
    @Getter
    private final Id taskId;

    @JsonCreator
    public DestroyTaskMessage(
        @JsonProperty("job") Id jobId,
        @JsonProperty("task") Id taskId
    ) {
        this.jobId = jobId;
        this.taskId = taskId;
    }

    public DestroyTaskMessage(@NonNull Task task) {
        this(task.getJobId(), task.getId());
    }
}
