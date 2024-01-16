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
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.NavigableMap;

@Getter
@JsonTypeName("partition")
@JsonPropertyOrder({"distributions", "keyMapping", "partIndices"})
public class PartitionParam extends AbstractParams {
    @JsonProperty("distributions")
    @JsonSerialize(keyUsing = ByteArrayUtils.ComparableByteArray.JacksonKeySerializer.class)
    @JsonDeserialize(keyUsing = ByteArrayUtils.ComparableByteArray.JacksonKeyDeserializer.class)
    private final NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions;
    @JsonProperty("tableDefinition")
    private final Table table;
    @Setter
    @JsonProperty("partIndices")
    @JsonSerialize(keyUsing = CommonId.JacksonKeySerializer.class)
    @JsonDeserialize(keyUsing = CommonId.JacksonKeyDeserializer.class)
    private Map<CommonId, Integer> partIndices;

    private final KeyValueCodec codec;
    private final DingoType schema;

    public PartitionParam(
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions,
        Table table
    ) {
        this.distributions = distributions;
        this.table = table;
        this.codec = CodecService.getDefault().createKeyValueCodec(table.tupleType(), table.keyMapping());
        this.schema = table.tupleType();
    }

}
