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
import io.dingodb.common.table.TupleMapping;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.impl.OutputIml;
import io.dingodb.exec.partition.PartitionStrategy;
import io.dingodb.meta.Location;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

@JsonTypeName("partition")
@JsonPropertyOrder({"strategy", "keyMapping", "outputs"})
public final class PartitionOperator extends AbstractOperator {
    @JsonProperty("strategy")
    private final PartitionStrategy strategy;
    @JsonProperty("keyMapping")
    private final TupleMapping keyMapping;
    @JsonProperty("outputs")
    @JsonSerialize(contentAs = OutputIml.class)
    @JsonDeserialize(contentAs = OutputIml.class)
    private Map<Object, Output> outputs;

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
    public synchronized boolean push(int pin, @Nonnull Object[] tuple) {
        Object partId = strategy.calcPartId(tuple, keyMapping);
        outputs.get(partId).push(tuple);
        return true;
    }

    @Override
    public void fin(int pin, Fin fin) {
        for (Output output : outputs.values()) {
            output.fin(fin);
        }
    }

    @Nonnull
    @Override
    public Collection<Output> getOutputs() {
        return outputs.values();
    }

    public void createOutputs(String tableName, @Nonnull Map<String, Location> partLocations) {
        outputs = new HashMap<>(partLocations.size());
        for (Map.Entry<String, Location> partLocation : partLocations.entrySet()) {
            OutputHint hint = OutputHint.of(tableName, partLocation.getKey());
            hint.setLocation(partLocation.getValue());
            Output output = OutputIml.of(this);
            output.setHint(hint);
            outputs.put(partLocation.getKey(), output);
        }
    }
}
