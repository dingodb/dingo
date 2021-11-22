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

package io.dingodb.exec.operator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.impl.OutputIml;

import java.util.List;

/**
 * Sole out operator has only one output.
 */
public abstract class SoleOutOperator extends AbstractOperator {
    @JsonProperty("output")
    @JsonSerialize(as = OutputIml.class)
    @JsonDeserialize(as = OutputIml.class)
    protected final Output output;

    protected SoleOutOperator() {
        super();
        output = OutputIml.of(this);
    }

    @Override
    public List<Output> getOutputs() {
        return ImmutableList.of(output);
    }
}
