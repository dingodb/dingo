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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.vector.VectorSearchResponse;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

@Slf4j
@JsonTypeName("scan")
@JsonPropertyOrder({
    "table", "part", "schema", "schemaVersion", "keyMapping", "filter", "selection", "indexId", "indexRegionId"
})
public final class PartVectorOperator extends PartIteratorSourceOperator {

    private final KeyValueCodec codec;

    @JsonProperty("tableDefinition")
    private final TableDefinition tableDefinition;

    @JsonProperty("distributions")
    @JsonSerialize(keyUsing = ByteArrayUtils.ComparableByteArray.JacksonKeySerializer.class)
    @JsonDeserialize(keyUsing = ByteArrayUtils.ComparableByteArray.JacksonKeyDeserializer.class)
    private final NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions;

    @JsonProperty("indexId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private CommonId indexId;

    @JsonProperty("indexRegionId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private CommonId indexRegionId;

    @JsonProperty("floatArray")
    private Float[] floatArray;

    @JsonProperty("topN")
    private int topN;

    @JsonProperty("parameterMap")
    private Map<String, Object> parameterMap;

    @JsonCreator
    public PartVectorOperator(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") CommonId partId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("schemaVersion") int schemaVersion,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("filter") SqlExpr filter,
        @JsonProperty("selection") TupleMapping selection,
        @JsonProperty("tableDefinition") TableDefinition tableDefinition,
        @JsonProperty("distributions") NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions,
        @JsonProperty("indexId") CommonId indexId,
        @JsonProperty("indexRegionId") CommonId indexRegionId,
        @JsonProperty("floatArray") Float[] floatArray,
        @JsonProperty("topN") int topN,
        @JsonProperty("topN") Map<String, Object> parameterMap
    ) {
        super(tableId, partId, schema, schemaVersion, keyMapping, filter, selection);
        this.codec = CodecService.getDefault().createKeyValueCodec(tableDefinition);
        this.tableDefinition = tableDefinition;
        this.distributions = distributions;
        this.indexId = indexId;
        this.indexRegionId = indexRegionId;
        this.floatArray = floatArray;
        this.topN = topN;
        this.parameterMap = parameterMap;
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator() {
        StoreInstance instance = StoreService.getDefault().getInstance(tableId, indexRegionId);
        List<Object[]> results = new ArrayList<>();

        // Get all table data response
        List<VectorSearchResponse> searchResponseList = instance.vectorSearch(indexId, floatArray, topN, parameterMap);
        for (VectorSearchResponse response : searchResponseList) {
            CommonId regionId = PartitionService.getService(
                    Optional.ofNullable(tableDefinition.getPartDefinition())
                        .map(PartitionDefinition::getFuncName)
                        .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME)).
                calcPartId(response.getKey(), distributions);
            StoreInstance storeInstance = StoreService.getDefault().getInstance(tableId, regionId);
            KeyValue keyValue = storeInstance.get(response.getKey());
            try {
                Object[] decode = codec.decode(keyValue);
                Object[] result = Arrays.copyOf(decode, decode.length + 1);
                result[decode.length] = response.getDistance();
                results.add(result);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return results.iterator();
    }

}
