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
import io.dingodb.common.Location;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.impl.OutputIml;
import io.dingodb.exec.operator.hash.HashStrategy;
import io.dingodb.exec.operator.hash.SimpleHashStrategy;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;

@JsonTypeName("hash")
@JsonPropertyOrder({"strategy", "keyMapping", "outputs"})
public class HashOperator extends FanOutOperator {
    @JsonProperty("strategy")
    private final HashStrategy strategy;
    @JsonProperty("keyMapping")
    private final TupleMapping keyMapping;

    @JsonCreator
    public HashOperator(
        @JsonProperty("strategy") HashStrategy strategy,
        @JsonProperty("keyMapping") TupleMapping keyMapping
    ) {
        super();
        this.strategy = strategy;
        this.keyMapping = keyMapping;
    }

    @Override
    public void init() {
        super.init();
        if (strategy instanceof SimpleHashStrategy) {
            ((SimpleHashStrategy) strategy).setOutputNum(outputs.size());
        } else {
            throw new IllegalArgumentException("Unsupported hash strategy \"" + strategy + "\".");
        }
    }

    @Override
    protected int calcOutputIndex(int pin, Object @NonNull [] tuple) {
        return strategy.selectOutput(keyMapping.revMap(tuple));
    }

    public void createOutputs(@NonNull Collection<Location> locations) {
        outputs = new ArrayList<>(locations.size());
        for (Location location : locations) {
            OutputHint hint = new OutputHint();
            hint.setLocation(location);
            Output output = OutputIml.of(this);
            output.setHint(hint);
            outputs.add(output);
        }
    }
}
