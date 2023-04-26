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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.codec.RawJsonDeserializer;
import io.dingodb.exec.converter.JsonConverter;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.meta.MetaService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@JsonTypeName("index")
@JsonPropertyOrder({"table", "schema", "indices", "indexValues", "filter", "selection", "output"})
public final class GetByIndexOperator extends FilterProjectSourceOperator {
    @JsonProperty("table")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId tableId;
    @JsonProperty("indices")
    private final TupleMapping indices;
    @JsonProperty("indexValues")
    private final List<Object[]> indexValues;

    public GetByIndexOperator(
        CommonId tableId,
        DingoType schema,
        TupleMapping indices,
        List<Object[]> indexValues,
        SqlExpr filter,
        TupleMapping selection
    ) {
        super(schema, filter, selection);
        this.tableId = tableId;
        this.indices = indices;
        this.indexValues = indexValues;
    }

    @JsonCreator
    public static @NonNull GetByIndexOperator fromJson(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("indices") TupleMapping indices,
        @JsonDeserialize(using = RawJsonDeserializer.class)
        @JsonProperty("indexValues") JsonNode jsonNode,
        @JsonProperty("filter") SqlExpr filter,
        @JsonProperty("selection") TupleMapping selection
    ) {
        return new GetByIndexOperator(
            tableId,
            schema,
            indices,
            RawJsonDeserializer.convertBySchema(jsonNode, schema.select(indices)),
            filter,
            selection
        );
    }

    // This method is only used by json serialization.
    @JsonProperty("indexValues")
    public List<Object[]> getJsonIndexValues() {
        return indexValues.stream()
            .map(i -> (Object[]) schema.select(indices).convertTo(i, JsonConverter.INSTANCE))
            .collect(Collectors.toList());
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator() {
        return indexValues.stream()
            .map(this::retrieveByIndex)
            .flatMap(Collection::stream)
            .collect(Collectors.toList())
            .iterator();
    }

    @Override
    public void init() {
        super.init();
        MetaService metaService = MetaService.root();
    }

    private List<Object[]> retrieveByIndex(Object[] indexValues) {
        Object[] data = new Object[schema.fieldCount()];
        Arrays.fill(data, null);
        boolean[] hasData = new boolean[schema.fieldCount()];
        Arrays.fill(hasData, false);
        for (int i = 0; i < indices.size(); ++i) {
            data[indices.get(i)] = indexValues[i];
            hasData[indices.get(i)] = true;
        }
        if (log.isDebugEnabled()) {
            log.debug("indices = {}, index values = {}", indices, schema.select(indices).format(indexValues));
            log.debug("data = {}, hasData = {}", schema.format(data), hasData);
        }
        return null;
    }
}
