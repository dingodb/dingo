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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.jackson.DingoValuesSerializer;
import io.dingodb.common.table.TupleSchema;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

@JsonTypeName("values")
@JsonPropertyOrder({"tuples", "output"})
public final class ValuesOperator extends IteratorSourceOperator {
    @JsonSerialize(using = DingoValuesSerializer.class)
    @JsonProperty("tuples")
    @Getter
    private final List<Object[]> tuples;
    @JsonProperty("schema")
    @Getter
    private final TupleSchema schema;

    @JsonCreator
    public ValuesOperator(
        @JsonProperty("tuples") List<Object[]> tuples,
        @JsonProperty("schema") TupleSchema schema
    ) {
        super();
        // Convert to recover the real type lost in JSON serialization/deserialization.
        this.tuples = tuples.stream()
            .map(schema::convert)
            .collect(Collectors.toList());
        this.schema = schema;
    }

    @Override
    public void init() {
        super.init();
        iterator = tuples.iterator();
    }

}
