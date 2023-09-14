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
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.codec.RawJsonDeserializer;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.partition.PartitionStrategy;
import io.dingodb.exec.table.PartInKvStore;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static io.dingodb.common.util.Utils.calculatePrefixCount;

@Slf4j
@JsonTypeName("index")
@JsonPropertyOrder({"indexTableId", "schema", "indices", "indexValues", "filter", "selection", "output"})
public final class GetByIndexOperator extends PartIteratorSourceOperator {
    @JsonProperty("indexTableId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId indexTableId;

    @JsonProperty("part")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId partId;

    @JsonProperty("tableId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId tableId;

    @JsonProperty("indices")
    private final TupleMapping indices;
    @JsonProperty("indexValues")
    private final List<Object[]> indexValues;

    @JsonProperty("isLookup")
    private final boolean isLookup;

    @JsonProperty("isUnique")
    private final boolean isUnique;

    @JsonProperty("indexDefinition")
    private final TableDefinition indexDefinition;
    private final TableDefinition tableDefinition;

    private final KeyValueCodec codec;

    private KeyValueCodec lookupCodec;

    private PartitionStrategy<CommonId, byte[]> strategy;

    public GetByIndexOperator(
        CommonId indexTableId,
        CommonId partId,
        CommonId tableId,
        TupleMapping indices,
        List<Object[]> indexValues,
        SqlExpr filter,
        TupleMapping selection,
        Boolean isUnique,
        PartitionStrategy<CommonId, byte[]> strategy,
        KeyValueCodec codec,
        TableDefinition indexDefinition,
        TableDefinition tableDefinition,
        boolean isLookup
    ) {
        super(indexTableId, partId, tableDefinition.getDingoType(), indices, filter, selection);
        this.indexTableId = indexTableId;
        this.partId = partId;
        this.tableId = tableId;
        this.indices = indices;
        this.indexValues = indexValues;
        this.isUnique = isUnique;
        // Determine if it is necessary : lookup table
        this.strategy = strategy;
        this.codec = codec;
        this.indexDefinition = indexDefinition;
        this.tableDefinition = tableDefinition;
        this.isLookup = isLookup;
    }

    @JsonCreator
    public static @NonNull GetByIndexOperator fromJson(
        @JsonProperty("indexTableId") CommonId indexTableId,
        @JsonProperty("partId") CommonId partId,
        @JsonProperty("tableId") CommonId tableId,
        @JsonProperty("indices") TupleMapping indices,
        @JsonDeserialize(using = RawJsonDeserializer.class)
        @JsonProperty("indexValues") JsonNode jsonNode,
        @JsonProperty("filter") SqlExpr filter,
        @JsonProperty("selection") TupleMapping selection,
        @JsonProperty("isUnique") Boolean isUnique,
        @JsonProperty("strategy") PartitionStrategy<CommonId, byte[]> strategy,
        @JsonProperty("codec") KeyValueCodec codec,
        @JsonProperty("indexDefinition") TableDefinition indexDefinition,
        @JsonProperty("tableDefinition") TableDefinition tableDefinition,
        @JsonProperty("isLookup") boolean isLookup
    ) {
        return new GetByIndexOperator(
            tableId,
            partId,
            tableId,
            indices,
            RawJsonDeserializer.convertBySchema(jsonNode, tableDefinition.getDingoType().select(indices)),
            filter,
            selection,
            isUnique,
            strategy,
            codec,
            indexDefinition,
            tableDefinition,
            isLookup
        );
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator() {
        List<Iterator<Object[]>> iteratorList = scan();
        List<Object[]> objectList = new ArrayList<>();
        for (Iterator<Object[]> iterator : iteratorList) {
            while (iterator.hasNext()) {
                Object[] objects = iterator.next();
                if (isLookup) {
                    Object[] val = lookUp(objects);
                    if (val != null) {
                        objectList.add(val);
                    }
                } else {
                    objectList.add(transformTuple(objects));
                }
            }
        }
        return objectList.iterator();
    }

    @Override
    public void init() {
        super.init();
        part = new PartInKvStore(
            StoreService.getDefault().getInstance(indexTableId, partId, indexDefinition),
            codec
        );
        lookupCodec = CodecService.getDefault().createKeyValueCodec(tableDefinition.getColumns());
    }

    private List<Iterator<Object[]>> scan() {
        try {
            List<Iterator<Object[]>> iteratorList = new ArrayList<>();
            for (Object[] tuple : indexValues) {
                byte[] keys = codec.encodeKeyPrefix(tuple, calculatePrefixCount(tuple));
                iteratorList.add(part.scan(keys));
            }
            return iteratorList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Object[] lookUp(Object[] tuples) {
        Object[] keyTuples = new Object[tableDefinition.getColumnsCount()];
        for (int i = 0; i < indices.getMappings().length; i ++) {
            keyTuples[indices.get(i)] = tuples[i];
        }
        try {
            byte[] keys = lookupCodec.encodeKey(keyTuples);
            CommonId regionId = strategy.calcPartId(keys);
            StoreInstance storeInstance = StoreService.getDefault().getInstance(tableId, regionId);
            return lookupCodec.decode(storeInstance.get(keys));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Object[] transformTuple(Object[] tuple) {
        Object[] response = new Object[tableDefinition.getColumnsCount()];
        List<Integer> selectedColumns = mapping(selection, tableDefinition, indexDefinition);
        for (int i = 0; i < selection.size(); i ++) {
            response[selection.get(i)] = tuple[selectedColumns.get(i)];
        }
        return response;
    }

    private static List<Integer> mapping(TupleMapping selection, TableDefinition td, TableDefinition index) {
        Integer[] mappings = new Integer[selection.size()];
        for (int i = 0; i < selection.size(); i ++) {
            ColumnDefinition columnDefinition = td.getColumn(selection.get(i));
            mappings[i] = index.getColumnIndex(columnDefinition.getName());
        }
        return Arrays.asList(mappings);
    }

}
