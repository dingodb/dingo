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

import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;

public abstract class FanOutOperator extends AbstractOperator {
    @JsonProperty("outputs")
    @JsonSerialize(contentAs = OutputIml.class)
    @JsonDeserialize(contentAs = OutputIml.class)
    protected List<Output> outputs;

    protected abstract int calcOutputIndex(int pin, @Nonnull Object[] tuple);

    @Override
    public synchronized boolean push(int pin, @Nonnull Object[] tuple) {
        int index = calcOutputIndex(pin, tuple);
        return outputs.get(index).push(tuple);
    }

    @Override
    public synchronized void fin(int pin, Fin fin) {
        for (Output output : outputs) {
            output.fin(fin);
        }
    }

    @Nonnull
    @Override
    public Collection<Output> getOutputs() {
        return outputs;
    }
}
