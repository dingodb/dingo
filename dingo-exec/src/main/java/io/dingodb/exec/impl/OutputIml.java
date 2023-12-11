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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.exec.base.Input;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import lombok.Getter;
import lombok.Setter;
import org.checkerframework.checker.nullness.qual.NonNull;

public class OutputIml implements Output {
    @Getter
    @Setter
    @JsonValue
    private Input link;
    @Getter
    @Setter
    private Operator operator;
    @Getter
    @Setter
    private OutputHint hint;

    private OutputIml() {
    }

    @JsonCreator
    public OutputIml(
        @JsonSerialize(using = CommonId.JacksonSerializer.class)
        @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
        @JsonProperty("operator") CommonId operatorId,
        @JsonProperty("pin") int pin) {
        this.link = new Input(operatorId, pin);
    }

    public static @NonNull OutputIml of(Operator operator) {
        OutputIml outputIml = new OutputIml();
        outputIml.setOperator(operator);
        return outputIml;
    }
}
