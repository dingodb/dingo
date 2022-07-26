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
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

@JsonTypeName("values")
@JsonPropertyOrder({"tuples", "output"})
public final class ValuesOperator extends IteratorSourceOperator {
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
        @JsonDeserialize(using = TuplesDeserializer.class)
        @Nonnull @JsonProperty("tuples") JsonNode jsonNode,
        @Nonnull @JsonProperty("schema") DingoType schema
    ) {
        return new ValuesOperator(TuplesDeserializer.convertBySchema(jsonNode, schema), schema);
    }

    // This method is only used by json serialization.
    @JsonProperty("tuples")
    public List<Object[]> getJsonTuples() {
        return tuples.stream()
            .map(i -> (Object[]) schema.convertTo(i, JsonConverter.INSTANCE))
            .collect(Collectors.toList());
    }

    @Override
    public void init() {
        super.init();
        iterator = tuples.iterator();
    }
}
