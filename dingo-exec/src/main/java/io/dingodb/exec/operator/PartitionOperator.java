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
import io.dingodb.common.table.TupleMapping;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.impl.OutputIml;
import io.dingodb.exec.partition.PartitionStrategy;
import io.dingodb.meta.Location;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

@JsonTypeName("partition")
@JsonPropertyOrder({"strategy", "keyMapping", "partIndices", "outputs"})
public final class PartitionOperator extends FanOutOperator {
    @JsonProperty("strategy")
    private final PartitionStrategy strategy;
    @JsonProperty("keyMapping")
    private final TupleMapping keyMapping;
    @JsonProperty("partIndices")
    private Map<Object, Integer> partIndices;

    @JsonCreator
    public PartitionOperator(
        @JsonProperty("strategy") PartitionStrategy strategy,
        @JsonProperty("keyMapping") TupleMapping keyMapping
    ) {
        super();
        this.strategy = strategy;
        this.keyMapping = keyMapping;
    }

    @Override
    protected int calcOutputIndex(int pin, @Nonnull Object[] tuple) {
        Object partId = strategy.calcPartId(tuple, keyMapping);
        return partIndices.get(partId);
    }

    public void createOutputs(@Nonnull Map<String, Location> partLocations) {
        int size = partLocations.size();
        outputs = new ArrayList<>(size);
        partIndices = new HashMap<>(size);
        for (Map.Entry<String, Location> entry : partLocations.entrySet()) {
            Object partId = entry.getKey();
            Output output = OutputIml.of(this);
            OutputHint hint = new OutputHint();
            hint.setLocation(entry.getValue());
            hint.setPartId(partId);
            output.setHint(hint);
            outputs.add(output);
            partIndices.put(partId, outputs.size() - 1);
        }
    }
}
