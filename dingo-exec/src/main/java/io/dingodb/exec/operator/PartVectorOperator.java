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
import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.partition.PartitionStrategy;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
@JsonTypeName("scan")
@JsonPropertyOrder({
    "table", "part", "schema", "keyMapping", "filter", "selection", "indexId", "indexRegionId"
})
public final class PartVectorOperator extends PartIteratorSourceOperator {

    private final KeyValueCodec codec;

    @JsonProperty("tableDefinition")
    private final TableDefinition tableDefinition;

    @JsonProperty("strategy")
    private final PartitionStrategy<CommonId, byte[]> strategy;

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

    @JsonCreator
    public PartVectorOperator(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") CommonId partId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("filter") SqlExpr filter,
        @JsonProperty("selection") TupleMapping selection,
        @JsonProperty("tableDefinition") TableDefinition tableDefinition,
        @JsonProperty("strategy") PartitionStrategy<CommonId, byte[]> strategy,
        @JsonProperty("indexId") CommonId indexId,
        @JsonProperty("indexRegionId") CommonId indexRegionId,
        @JsonProperty("floatArray") Float[] floatArray,
        @JsonProperty("topN") int topN
    ) {
        super(tableId, partId, schema, keyMapping, filter, selection);
        this.codec = CodecService.getDefault().createKeyValueCodec(tableDefinition);
        this.tableDefinition = tableDefinition;
        this.strategy = strategy;
        this.indexId = indexId;
        this.indexRegionId = indexRegionId;
        this.floatArray = floatArray;
        this.topN = topN;
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator() {
        StoreInstance instance = StoreService.getDefault().getInstance(tableId, indexRegionId);
        Iterator<Object[]> iterator;

        List<Object[]> results = new ArrayList<>();

        List<byte[]> keyList = instance.vectorSearch(indexId, floatArray, topN);
        for (byte[] key : keyList) {
            CommonId regionId = strategy.calcPartId(key);
            StoreInstance storeInstance = StoreService.getDefault().getInstance(tableId, regionId);
            KeyValue keyValue = storeInstance.get(key);
            try {
                results.add(codec.decode(keyValue));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return results.iterator();
    }

}
