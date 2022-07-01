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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.JsonConverter;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

@JsonTypeName("values")
@JsonPropertyOrder({"tuples", "output"})
public final class ValuesOperator extends IteratorSourceOperator {
    @JsonProperty("tuples")
    @Getter
    private final List<Object[]> tuples;
    @JsonProperty("schema")
    @Getter
    private final DingoType schema;

    public ValuesOperator(@Nonnull List<Object[]> tuples, @Nonnull DingoType schema) {
        super();
        this.tuples = tuples;
        this.schema = schema;
    }

    @Nonnull
    @JsonCreator
    public static ValuesOperator fromJson(
        @Nonnull @JsonProperty("tuples") List<Object[]> tuples,
        @Nonnull @JsonProperty("schema") DingoType schema
    ) {
        // Recover the real type lost in JSON serialization/deserialization.
        List<Object[]> newTuples = tuples.stream()
            .map(i -> (Object[]) schema.convertFrom(i, JsonConverter.INSTANCE))
            .collect(Collectors.toList());
        return new ValuesOperator(newTuples, schema);
    }

    @Override
    public void init() {
        super.init();
        iterator = tuples.iterator();
    }
}
