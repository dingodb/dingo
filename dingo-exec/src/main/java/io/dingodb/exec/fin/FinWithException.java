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

package io.dingodb.exec.fin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.expr.json.runtime.Parser;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;

public class FinWithException implements Fin {
    public static final Parser PARSER = Parser.JSON;

    @Getter
    @JsonValue
    TaskStatus taskStatus;

    @JsonCreator
    public FinWithException(TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
    }

    public static FinWithException deserialize(ByteArrayInputStream is) throws IOException {
        return PARSER.parse(is, FinWithException.class);
    }

    public static @NonNull FinWithException of(TaskStatus taskStatus) {
        return new FinWithException(taskStatus);
    }

    @Override
    public void writeStream(@NonNull OutputStream os) throws IOException {
        PARSER.writeStream(os, this);
    }

    @Override
    public String detail() {
        return taskStatus.getErrorMsg();
    }

    @Override
    public String toString() {
        try {
            return PARSER.stringify(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
