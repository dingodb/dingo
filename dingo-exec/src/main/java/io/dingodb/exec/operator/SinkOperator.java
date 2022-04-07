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

import com.google.common.collect.ImmutableList;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.fin.Fin;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * Sink operator has only one input and no output.
 */
public abstract class SinkOperator extends AbstractOperator {
    protected abstract boolean push(Object[] tuple);

    @Override
    public synchronized boolean push(int pin, @Nonnull Object[] tuple) {
        return push(tuple);
    }

    protected abstract void fin(Fin fin);

    @Override
    public synchronized void fin(int pin, Fin fin) {
        fin(fin);
    }

    @Override
    public List<Output> getOutputs() {
        return ImmutableList.of();
    }
}
