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
import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.Services;
import io.dingodb.exec.codec.RawJsonDeserializer;
import io.dingodb.exec.converter.JsonConverter;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.table.PartInKvStore;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@JsonTypeName("get")
@JsonPropertyOrder({"table", "part", "schema", "schemaVersion", "keyMapping", "keys", "filter", "selection", "output"})
public final class GetByKeysOperator extends PartIteratorSourceOperator {
    private final List<Object[]> keyTuples;

    public GetByKeysOperator(
        CommonId tableId,
        CommonId partId,
        DingoType schema,
        int schemaVersion,
        TupleMapping keyMapping,
        List<Object[]> keyTuples,
        SqlExpr filter,
        TupleMapping selection
    ) {
        super(tableId, partId, schema, schemaVersion, keyMapping, filter, selection);
        this.keyTuples = keyTuples;
    }

    @JsonCreator
    public static @NonNull GetByKeysOperator fromJson(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") CommonId partId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("schemaVersion") int schemaVersion,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonDeserialize(using = RawJsonDeserializer.class)
        @JsonProperty("keys") JsonNode jsonNode,
        @JsonProperty("filter") SqlExpr filter,
        @JsonProperty("selection") TupleMapping selection
    ) {
        return new GetByKeysOperator(
            tableId,
            partId,
            schema,
            schemaVersion,
            keyMapping,
            RawJsonDeserializer.convertBySchema(jsonNode, schema.select(keyMapping)),
            filter,
            selection
        );
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator() {
        return keyTuples.stream()
            .map(part::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toList())
            .iterator();
    }

    // This method is only used by json serialization.
    @JsonProperty("keys")
    public List<Object[]> getJsonKeyTuples() {
        return keyTuples.stream()
            .map(i -> (Object[]) schema.convertTo(i, JsonConverter.INSTANCE))
            .collect(Collectors.toList());
    }

    @Override
    public void init() {
        super.init();
        part = new PartInKvStore(
            Services.KV_STORE.getInstance(tableId, partId),
            CodecService.getDefault().createKeyValueCodec(schemaVersion, schema, keyMapping)
        );
    }
}
