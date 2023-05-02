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
import io.dingodb.common.partition.Distribution;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.partition.RangeTupleDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.impl.OutputIml;
import io.dingodb.exec.partition.PartitionStrategy;
import io.dingodb.exec.partition.RangeStrategy;
import io.dingodb.meta.MetaService;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@JsonTypeName("partition")
@JsonPropertyOrder({"strategy", "keyMapping", "partIndices", "outputs"})
public final class PartitionOperator extends FanOutOperator {
    @JsonProperty("tableId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId tableId;
    @JsonProperty("strategy")
    private final PartitionStrategy<CommonId, byte[]> strategy;
    @JsonProperty("tableDefinition")
    private final TableDefinition tableDefinition;
    @JsonProperty("partIndices")
    @JsonSerialize(keyUsing = CommonId.JacksonKeySerializer.class)
    @JsonDeserialize(keyUsing = CommonId.JacksonKeyDeserializer.class)
    private Map<CommonId, Integer> partIndices;

    private final KeyValueCodec codec;

    @JsonCreator
    public PartitionOperator(
        @JsonProperty("tableId") CommonId tableId,
        @JsonProperty("strategy") PartitionStrategy<CommonId, byte[]> strategy,
        @JsonProperty("tableDefinition") TableDefinition tableDefinition
    ) {
        super();
        this.tableId = tableId;
        this.strategy = strategy;
        this.tableDefinition = tableDefinition;
        this.codec = CodecService.getDefault().createKeyValueCodec(tableId, tableDefinition);
    }

    @Override
    protected int calcOutputIndex(int pin, Object @NonNull [] tuple) {
        CommonId partId = strategy.calcPartId(tuple, wrap(codec::encodeKey));
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
