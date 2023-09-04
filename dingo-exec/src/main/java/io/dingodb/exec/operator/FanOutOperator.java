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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.impl.OutputIml;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.List;

@Slf4j
public abstract class FanOutOperator extends AbstractOperator {
    @JsonProperty("outputs")
    @JsonSerialize(contentAs = OutputIml.class)
    @JsonDeserialize(contentAs = OutputIml.class)
    protected List<Output> outputs;

    protected abstract int calcOutputIndex(int pin, Object @NonNull [] tuple);

    @Override
    public synchronized boolean push(int pin, Object @NonNull [] tuple) {
        int index = calcOutputIndex(pin, tuple);
        if (log.isDebugEnabled()) {
            log.debug("Tuple is pushing to output {}.", index);
        }
        return outputs.get(index).push(tuple);
    }

    @Override
    public synchronized void fin(int pin, Fin fin) {
        if (log.isDebugEnabled()) {
            log.debug("Got FIN, push it to {} outputs", outputs.size());
        }
        for (Output output : outputs) {
            output.fin(fin);
        }
    }

    @Override
    public @NonNull Collection<Output> getOutputs() {
        return outputs;
    }
}
