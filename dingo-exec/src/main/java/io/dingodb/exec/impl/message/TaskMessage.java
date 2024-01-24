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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.exec.impl.JobImpl;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(CreateTaskMessage.class),
    @JsonSubTypes.Type(RunTaskMessage.class),
    @JsonSubTypes.Type(DestroyTaskMessage.class),
})
@Slf4j
public abstract class TaskMessage {
    public static TaskMessage fromBytes(byte[] bytes) throws JsonProcessingException {
//        String str = new String(bytes, StandardCharsets.UTF_8);
//        if (log.isDebugEnabled()) {
//            log.debug("TaskMessage deserialized, content: {}", str);
//        }
//        return JobImpl.PARSER.parse(str, TaskMessage.class);
        return ProtostuffCodec.read(bytes);
    }

    public byte[] toBytes() {
//        return toString().getBytes(StandardCharsets.UTF_8);
        return ProtostuffCodec.write(this);
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
