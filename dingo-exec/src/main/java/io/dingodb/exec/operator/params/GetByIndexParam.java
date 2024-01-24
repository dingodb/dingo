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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.table.PartInKvStore;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreService;
import lombok.Getter;

import java.util.List;
import java.util.NavigableMap;

@Getter
@JsonTypeName("index")
@JsonPropertyOrder({"indexTableId", "schema", "keyMapping", "indexValues", "filter", "selection"})
public class GetByIndexParam extends FilterProjectSourceParam {

    @JsonProperty("indexTableId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId indexTableId;
    @JsonProperty("indexValues")
    private final List<Object[]> indexValues;
    @JsonProperty("isLookup")
    private final boolean isLookup;
    @JsonProperty("isUnique")
    private final boolean isUnique;
    @JsonProperty("indexDefinition")
    private final Table index;
    private final Table table;
    private final KeyValueCodec codec;
    private transient KeyValueCodec lookupCodec;
    private NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges;

    public GetByIndexParam(
        CommonId indexTableId,
        CommonId partId,
        CommonId tableId,
        TupleMapping keyMapping,
        List<Object[]> indexValues,
        SqlExpr filter,
        TupleMapping selection,
        boolean isUnique,
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges,
        KeyValueCodec codec,
        Table index,
        Table table,
        boolean isLookup
    ) {
        super(tableId, partId, table.tupleType(), filter, selection, keyMapping);
        this.indexTableId = indexTableId;
        this.indexValues = indexValues;
        this.isLookup = isLookup;
        this.isUnique = isUnique;
        this.index = index;
        this.table = table;
        this.codec = codec;
        this.ranges = ranges;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        part = new PartInKvStore(
            StoreService.getDefault().getInstance(indexTableId, partId, index.getTableId()),
            codec
        );
        lookupCodec = CodecService.getDefault().createKeyValueCodec(table.tupleType(), table.keyMapping());
    }
}
