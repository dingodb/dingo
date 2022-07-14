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

package io.dingodb.exec.aggregate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.type.DingoType;
import io.dingodb.expr.runtime.evaluator.arithmetic.MinEvaluatorFactory;

import javax.annotation.Nonnull;

@JsonTypeName("min")
public class MinAgg extends UnityEvaluatorAgg {
    @JsonCreator
    public MinAgg(
        @JsonProperty("index") int index,
        @Nonnull @JsonProperty("type") DingoType type
    ) {
        super(index, type);
        setEvaluator(MinEvaluatorFactory.INSTANCE);
    }
}
