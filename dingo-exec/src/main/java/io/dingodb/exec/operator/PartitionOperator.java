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
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.impl.OutputIml;
import io.dingodb.net.Location;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

@JsonTypeName("partition")
@JsonPropertyOrder({"table", "outputs"})
public final class PartitionOperator extends AbstractOperator {
    @JsonProperty("table")
    private final String tableName;
    @JsonProperty("outputs")
    @JsonSerialize(contentAs = OutputIml.class)
    @JsonDeserialize(contentAs = OutputIml.class)
    @Getter
    private List<Output> outputs;

    @JsonCreator
    public PartitionOperator(
        @JsonProperty("table") String tableName
    ) {
        super();
        this.tableName = tableName;
    }

    @Override
    public synchronized boolean push(int pin, @Nonnull Object[] tuple) {
        // TODO: partition and push next level.
        return false;
    }

    @Override
    public void fin(int pin, Fin fin) {
        // TODO:
    }

    public void createOutputs(@Nonnull Map<Object, Location> partLocations) {
        outputs = new ArrayList<>(partLocations.size());
        for (Map.Entry<Object, Location> partLocation : partLocations.entrySet()) {
            OutputHint hint = new OutputHint();
            hint.setTableName(tableName);
            hint.setPartId(partLocation.getKey());
            hint.setLocation(partLocation.getValue());
            Output output = OutputIml.of(this);
            output.setHint(hint);
            outputs.add(output);
        }
    }
}
