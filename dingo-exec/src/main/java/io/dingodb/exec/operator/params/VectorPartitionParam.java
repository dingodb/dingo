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
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.NavigableMap;

@Getter
@JsonTypeName("vectorPartition")
@JsonPropertyOrder({"tableId"})
public class VectorPartitionParam extends AbstractParams {

    @JsonProperty("tableId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId tableId;
    private final NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions;
    @Setter
    private Map<CommonId, Integer> partIndices;
    private Integer index;
    private final KeyValueCodec codec;
    private final Table table;

    public VectorPartitionParam(
        CommonId tableId,
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions,
        CommonId indexId,
        Integer index,
        Table table
    ) {
        this.tableId = tableId;
        this.distributions = distributions;
        this.index = index;
        DingoType dingoType = new LongType(false);
        TupleType tupleType = DingoTypeFactory.tuple(new DingoType[]{dingoType});
        TupleMapping outputKeyMapping = TupleMapping.of(new int[] {0});
        this.codec = CodecService.getDefault().createKeyValueCodec(indexId, tupleType, outputKeyMapping);
        this.table = table;
    }

}
