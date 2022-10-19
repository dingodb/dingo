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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.JsonConverter;
import io.dingodb.exec.codec.RawJsonDeserializer;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@JsonTypeName("values")
@JsonPropertyOrder({"tuples", "output"})
public final class ValuesOperator extends IteratorSourceOperator {
    @Getter
    private final List<Object[]> tuples;
    @JsonProperty("schema")
    @Getter
    private final DingoType schema;

    public ValuesOperator(@NonNull List<Object[]> tuples, @NonNull DingoType schema) {
        super();
        this.tuples = tuples;
        this.schema = schema;
    }

    @JsonCreator
    public static @NonNull ValuesOperator fromJson(
        @JsonDeserialize(using = RawJsonDeserializer.class)
        @JsonProperty("tuples") JsonNode jsonNode,
        @JsonProperty("schema") DingoType schema
    ) {
        return new ValuesOperator(RawJsonDeserializer.convertBySchema(jsonNode, schema), schema);
    }

    // This method is only used by json serialization.
    @JsonProperty("tuples")
    public List<Object[]> getJsonTuples() {
        return tuples.stream()
            .map(i -> (Object[]) schema.convertTo(i, JsonConverter.INSTANCE))
            .collect(Collectors.toList());
    }

    @Override
    protected @NonNull Iterator<Object[]> createIterator() {
        return tuples.iterator();
    }
}
