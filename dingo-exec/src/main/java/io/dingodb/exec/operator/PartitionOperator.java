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
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.impl.OutputIml;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.meta.MetaService;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@JsonTypeName("partition")
@JsonPropertyOrder({"distributions", "keyMapping", "partIndices", "outputs"})
public final class PartitionOperator extends FanOutOperator {
    @JsonProperty("distributions")
    @JsonSerialize(keyUsing = ComparableByteArray.JacksonKeySerializer.class)
    @JsonDeserialize(keyUsing = ComparableByteArray.JacksonKeyDeserializer.class)
    private final NavigableMap<ComparableByteArray, RangeDistribution> distributions;
    @JsonProperty("tableDefinition")
    private final TableDefinition tableDefinition;
    @JsonProperty("partIndices")
    @JsonSerialize(keyUsing = CommonId.JacksonKeySerializer.class)
    @JsonDeserialize(keyUsing = CommonId.JacksonKeyDeserializer.class)
    private Map<CommonId, Integer> partIndices;

    private final KeyValueCodec codec;
    private final DingoType schema;

    @JsonCreator
    public PartitionOperator(
        @JsonProperty("distributions") NavigableMap<ComparableByteArray, RangeDistribution> distributions,
        @JsonProperty("tableDefinition") TableDefinition tableDefinition
    ) {
        super();
        this.distributions = distributions;
        this.tableDefinition = tableDefinition;
        this.codec = CodecService.getDefault().createKeyValueCodec(tableDefinition);
        this.schema = tableDefinition.getDingoType();
    }

    @Override
    protected int calcOutputIndex(int pin, Object @NonNull [] tuple) {
        Object[] newTuple = (Object[]) schema.convertFrom(
            Arrays.copyOf(tuple, schema.fieldCount()),
            ValueConverter.INSTANCE
        );
        CommonId partId = PartitionService.getService(
                Optional.ofNullable(tableDefinition.getPartDefinition())
                    .map(PartitionDefinition::getFuncName)
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME)).
            calcPartId(newTuple, wrap(codec::encodeKey), distributions);
        return partIndices.get(partId);
    }

    public void createOutputs(@NonNull NavigableMap<ComparableByteArray, RangeDistribution> distributions) {
        int size = distributions.size();
        outputs = new ArrayList<>(size);
        partIndices = new HashMap<>(size);
        for (RangeDistribution distribution : distributions.values()) {
            Output output = OutputIml.of(this);
            OutputHint hint = new OutputHint();
            hint.setLocation(MetaService.root().currentLocation());
            hint.setPartId(distribution.id());
            output.setHint(hint);
            outputs.add(output);
            partIndices.put(distribution.id(), outputs.size() - 1);
        }
    }
}
